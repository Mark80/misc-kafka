package kafka_stream.zmart.producer

import java.util.Properties

import kafka_stream.zmart.purchase.PurchaseKeyPartitioner
import kafka_stream.zmart.serdes.SerializerFactory
import kafka_stream.zmart.stock.Stock
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class StockProducer(implicit ex: ExecutionContext) {

  private val keySerializer = new StringSerializer
  private val valueSerializer = SerializerFactory[Stock]

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[PurchaseKeyPartitioner].getName)
  props.put(ProducerConfig.ACKS_CONFIG, "1")
  props.put(ProducerConfig.RETRIES_CONFIG, "3")

  private val kafkaProducer = new KafkaProducer[String, Stock](props, keySerializer, valueSerializer)

  def send(stock: Stock): Future[RecordMetadata] = {

    val transactionRecord =
      new ProducerRecord[String, Stock]("stock", stock.symbol, stock)

    Future {
      kafkaProducer
        .send(transactionRecord,
              (_: RecordMetadata, exception: Exception) =>
                if (exception != null)
                  exception.printStackTrace())
        .get()
    }

  }

  def close() =
    kafkaProducer.close()

}
