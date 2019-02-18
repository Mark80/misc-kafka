package kafka_stream.zmart.producer

import java.util.Properties

import kafka_stream.zmart.purchase.{Purchase, PurchaseKey, PurchaseKeyPartitioner}
import kafka_stream.zmart.serdes.SerializerFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.{ExecutionContext, Future}

class CustomerProducer(implicit ex: ExecutionContext) {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[PurchaseKeyPartitioner].getName)
  props.put(ProducerConfig.ACKS_CONFIG, "1")
  props.put(ProducerConfig.RETRIES_CONFIG, "3")

  private val kafkaProducer = new KafkaProducer[String, String](props, new StringSerializer, new StringSerializer)

  def send(customerId: String, name: String): Future[RecordMetadata] = {

    val transactionRecord =
      new ProducerRecord[String, String]("customer", customerId, name)
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
