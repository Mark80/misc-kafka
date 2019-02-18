package kafka_stream.zmart.producer

import java.util.Properties

import kafka_stream.zmart.serdes.SerializerFactory
import kafka_stream.zmart.stock.FinancialNews
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.{ExecutionContext, Future}

class NewsProducer(implicit executionContext: ExecutionContext) {

  private val keySerializer = new StringSerializer
  private val valueSerializer = SerializerFactory[FinancialNews]

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.ACKS_CONFIG, "1")
  props.put(ProducerConfig.RETRIES_CONFIG, "3")

  private val kafkaProducer = new KafkaProducer[String, FinancialNews](props, keySerializer, valueSerializer)

  def send(news: FinancialNews): Future[RecordMetadata] = {

    val key = news.industry
    val transactionRecord =
      new ProducerRecord[String, FinancialNews]("news", key, news)
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
