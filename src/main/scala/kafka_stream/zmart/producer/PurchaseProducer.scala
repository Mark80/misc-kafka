package kafka_stream.zmart.producer

import java.util.Properties

import kafka_stream.zmart.purchase.{Purchase, PurchaseKey, PurchaseKeyPartitioner}
import kafka_stream.zmart.serdes.SerializerFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

import scala.concurrent.{ExecutionContext, Future}

class PurchaseProducer(implicit ex: ExecutionContext) {

  private val keySerializer = SerializerFactory[PurchaseKey]
  private val valueSerializer = SerializerFactory[Purchase]

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[PurchaseKeyPartitioner].getName)
  props.put(ProducerConfig.ACKS_CONFIG, "1")
  props.put(ProducerConfig.RETRIES_CONFIG, "3")

  private val kafkaProducer = new KafkaProducer[PurchaseKey, Purchase](props, keySerializer, valueSerializer)

  def send(purchase: Purchase): Future[RecordMetadata] = {

    val key = PurchaseKey(purchase.customerId, purchase.date)
    val transactionRecord =
      new ProducerRecord[PurchaseKey, Purchase]("purchase", key, purchase)
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
