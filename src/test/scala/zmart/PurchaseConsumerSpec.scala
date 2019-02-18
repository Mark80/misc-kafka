package zmart

import java.time.{Duration, LocalDateTime}
import java.util
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import kafka_stream.zmart.purchase.Purchase
import kafka_stream.zmart.serdes.{DeserializerFactory, SerializerFactory}
import net.manub.embeddedkafka.{Consumers, EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

class PurchaseConsumerSpec
    extends WordSpec
    with Matchers
    with EmbeddedKafka
    with Consumers
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience {

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 9092)

  implicit val purchaseDeserializer = DeserializerFactory[Purchase]
  implicit val purchaseSerializer = SerializerFactory[Purchase]

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "2")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val duration = Duration.ofSeconds(15)

  "PurchaseConsumer" should {

    "consume purchase" in {

      withRunningKafka {

        val purchase = Purchase(
          customerId = "id",
          price = 1000,
          item = "book",
          zipCode = "zipCode",
          date = LocalDateTime.now(),
          cardNumber = "card number",
          employeeId = "emploeeId",
          firstName = "Name",
          lastName = "LastName",
          department = "department",
          quantity = 2,
          storeId = "storeId"
        )

        sendPurchase(purchase)

        val consumer = new KafkaConsumer[String, Purchase](props, new StringDeserializer, purchaseDeserializer)
        val topics = new util.ArrayList[String]()
        topics.add("purchase")
        consumer.subscribe(topics)

        val consumerRecord: ConsumerRecords[String, Purchase] = consumer.poll(duration)

        consumerRecord.count() shouldBe 1

      }

    }

  }

  private def sendPurchase(purchase: Purchase) = {
    val producer = aKafkaProducer[Purchase]
    val producerRecord = new ProducerRecord[String, Purchase]("purchase", "test", purchase)
    producer.send(producerRecord).get()
    producer.close()
  }
}

class PurchaseConsumer {

  @volatile
  private var stopConsuming = false
  private val threadPool = Executors.newFixedThreadPool(2)

  private val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "2")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private val purchaseDeserializer = DeserializerFactory[Purchase]

  def startConsuming: Unit = {

    val consumers = (1 to 2).map(_ => createConsumer)

    sys.addShutdownHook {
      stopConsuming = true
      threadPool.awaitTermination(3, TimeUnit.SECONDS)
      threadPool.isShutdown
    }

    consumers.foreach(threadPool.execute)

  }

  private def createConsumer =
    new Runnable {
      def run(): Unit = {
        val consumer = new KafkaConsumer[String, Purchase](props, new StringDeserializer, purchaseDeserializer)
        val topics = new util.ArrayList[String]()
        topics.add("purchase")
        while (!stopConsuming) {
          val batchMessages = consumer.poll(Duration.ofSeconds(5))
          logic(batchMessages)
        }
        consumer.close()
      }

    }

  private def logic(batchMessages: ConsumerRecords[String, Purchase]) =
    batchMessages.forEach(m => println(m.value()))

  def stopConsumer =
    stopConsuming = true

}
