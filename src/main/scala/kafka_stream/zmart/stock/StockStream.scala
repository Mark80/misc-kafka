package kafka_stream.zmart.stock

import java.util.Properties

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import kafka_stream.zmart.serdes.{CommonFormat, SerdeFactory}
import kafka_stream.zmart.stock.FixedSizePriorityQueue.queueFormat
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.collection.mutable

class StockStream {

  implicit val comparator: Ordering[ShareVolume] = (sv1: ShareVolume, sv2: ShareVolume) =>
    sv2.shares - sv1.shares

  implicit val stockSerde: Serde[Stock] = SerdeFactory[Stock]
  implicit val summarySerde = SerdeFactory[TransactionSummary]
  implicit val shareSerde = SerdeFactory[ShareVolume]
  implicit val newsSerde = SerdeFactory[FinancialNews]

  implicit val shareQueueFormat = queueFormat[ShareVolume]
  implicit def queueSerde(implicit format: RootJsonFormat[ShareVolume]) =
    SerdeFactory.apply[FixedSizePriorityQueue[ShareVolume]]
  implicit val consumedStock = Consumed.`with`(Serdes.String, stockSerde)
  implicit val producedStock = Produced.`with`(Serdes.String, stockSerde)
  implicit val producedShareVolume = Produced.`with`(Serdes.String, shareSerde)
  implicit val producedQueue = Produced.`with`(Serdes.String, queueSerde)
  implicit val serializedShareVolume = Serialized.`with`(Serdes.String, shareSerde)
  implicit val serializedStock = Serialized.`with`(Serdes.String, stockSerde)
  implicit val serializedSummary = Serialized.`with`(summarySerde, stockSerde)
  implicit val consumedNews = Consumed.`with`(Serdes.String, newsSerde)
  implicit val consumedPair = Consumed.`with`(Serdes.String, Serdes.String)

  implicit val materializeShare2 =
    Materialized.as[String, ShareVolume, ByteArrayKeyValueStore]("shareRepo")

  implicit val materializedQueue =
    Materialized.`with`[String, FixedSizePriorityQueue[ShareVolume], ByteArrayKeyValueStore](
      Serdes.String,
      queueSerde)

  implicit val materializeSummary =
    Materialized.`with`[TransactionSummary, Long, ByteArraySessionStore](summarySerde, Serdes.Long)

  implicit val joinedNews: Joined[String, TransactionSummary, FinancialNews] =
    Joined.`with`(Serdes.String, summarySerde, newsSerde)

  val props = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-application")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once")
    p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1")
    p.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1")

    p
  }

  def start(): (() => Unit, ReadOnlyKeyValueStore[String, ShareVolume]) = {

    val builder = new StreamsBuilder()

    val transactionStream: kstream.KStream[String, Stock] = builder
      .stream("stock")

    val shareVolume: KTable[String, ShareVolume] = transactionStream
      .mapValues(stock => ShareVolume.from(stock))
      .groupBy((_, value) => value.symbol)
      .reduce((a, b) => ShareVolume(a.symbol, a.shares + b.shares, a.industry))

    shareVolume.toStream.to("share-volume")
    shareVolume.toStream.peek((k, v) => println(s"$k $v")).to("tmp")

    topFiveStream(shareVolume)

    val sessionTable = windowsStream(transactionStream)

    sessionTable.toStream
      .print(
        Printed
          .toSysOut[Windowed[TransactionSummary], Long]
          .withLabel("Customer Transactions Counts"))

    val industrySummary: kstream.KStream[String, TransactionSummary] =
      sessionTable.toStream
        .map((window, count) => {
          val summary = window.key()
          val newKey = summary.industry
          (newKey, summary.copy(count = count))
        })

    val customerRepository = builder.globalTable[String, String]("customer")

    industrySummary
      .leftJoin(customerRepository)((_, summary) => summary.customerId,
                                    (summary, name) => s"customer $name with summary $summary")
      .print(Printed.toSysOut[String, String].withLabel("join-name"))

    val newsStream =
      builder
        .table[String, FinancialNews]("news")

    val joinNews =
      industrySummary
        .leftJoin(newsStream)((summary, news) =>
          s"${summary.count} shares purchased ${summary.symbol} related news $news")

    joinNews.print(Printed.toSysOut[String, String].withLabel("Joined news"))

    val stockKafkaStream = new KafkaStreams(builder.build(), props)

    stockKafkaStream.start()

    Thread.sleep(6000)

    val storeShare: ReadOnlyKeyValueStore[String, ShareVolume] =
      stockKafkaStream.store("shareRepo", QueryableStoreTypes.keyValueStore[String, ShareVolume]())

    (() => stockKafkaStream.close(), storeShare)
  }

  private def windowsStream(transactionStream: kstream.KStream[String, Stock])
    : KTable[Windowed[TransactionSummary], Long] = {
    val twentySeconds = 1000 * 20
    val fifteenMinutes = 1000 * 60 * 15
    transactionStream
      .groupBy((_, stock) => TransactionSummary.from(stock))
      .windowedBy(SessionWindows.`with`(twentySeconds).until(fifteenMinutes))
      .count()
  }

  private def topFiveStream(shareVolume: KTable[String, ShareVolume]): Unit = {
    val fixedQueue = new FixedSizePriorityQueue[ShareVolume](5)

    val topFive: KTable[String, FixedSizePriorityQueue[ShareVolume]] = shareVolume
    // la chiave diventa l'industry
    // è un groupby su una table quindi crea una
    //  KGroupedTable (un tipo di tabella) su cui poi fare aggregazione
      .groupBy((_, v) => (v.industry, v))
      .aggregate(fixedQueue)((_, v, _) => fixedQueue.add(v), (_, v, _) => fixedQueue.remove(v))

    topFive.toStream.to("top-five")
    topFive.toStream.peek((k, v) => println(s"$k $v")).to("top-five2")
  }
}

class FixedSizePriorityQueue[T](size: Int)(implicit ordering: Ordering[T]) {

  var queue = new mutable.PriorityQueue[T]

  def add(t: T): FixedSizePriorityQueue[T] = {
    queue += t
    val sortedList = queue.toList.sorted.reverse.take(size)
    queue = new mutable.PriorityQueue[T] ++ sortedList
    this
  }

  def remove(t: T): FixedSizePriorityQueue[T] = {
    queue = queue.filterNot(_ == t)
    this
  }

}

object FixedSizePriorityQueue extends DefaultJsonProtocol with SprayJsonSupport with CommonFormat {

  import spray.json._

  implicit def queueFormat[T](implicit valueFormat: RootJsonFormat[T], ordering: Ordering[T]) =
    new RootJsonFormat[FixedSizePriorityQueue[T]] {

      def write(obj: FixedSizePriorityQueue[T]): JsValue = {
        val list = obj.queue.toVector.map(_.toJson)
        JsArray(list)
      }

      def read(json: JsValue): FixedSizePriorityQueue[T] = json match {

        case JsArray(values: Vector[JsValue]) =>
          val elements: Vector[T] = values.map(_.convertTo[T])
          val queue = new FixedSizePriorityQueue[T](5)
          elements.foreach(el => queue.add(el))
          queue
      }
    }

}
