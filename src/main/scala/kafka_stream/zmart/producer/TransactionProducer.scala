package kafka_stream.zmart.producer

import java.util.Properties

import kafka_stream.zmart.purchase.{Purchase, PurchaseKey, PurchaseKeyPartitioner, Transaction}
import kafka_stream.zmart.serdes.SerializerFactory
import org.apache.kafka.clients.producer._

import scala.concurrent.{ExecutionContext, Future}

class TransactionProducer(implicit executionContext: ExecutionContext) {

  private val keySerializer = SerializerFactory[PurchaseKey]
  private val valueSerializer = SerializerFactory[Transaction]

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[PurchaseKeyPartitioner].getName)
  props.put(ProducerConfig.ACKS_CONFIG, "1")
  props.put(ProducerConfig.RETRIES_CONFIG, "3")

  private val kafkaProducer = new KafkaProducer[PurchaseKey, Transaction](props, keySerializer, valueSerializer)

  def send(purchase: Purchase): Future[RecordMetadata] = {

    val key = PurchaseKey(purchase.customerId, purchase.date)
    val transaction = Transaction(purchase.customerId, purchase.price)
    val transactionRecord =
      new ProducerRecord[PurchaseKey, Transaction]("transaction", key, transaction)
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
