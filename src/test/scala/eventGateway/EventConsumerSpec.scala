package eventGateway

import java.time.Duration
import java.util
import java.util.Properties

import net.manub.embeddedkafka.{Consumers, EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import service.model.CreatedOrder

class EventConsumerSpec extends WordSpec with Matchers with EmbeddedKafka with Consumers with BeforeAndAfterAll {

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 9092)

  implicit val eventSerializer = new CreatedOrderSerializer

  implicit val keyDeserializer = new StringDeserializer
  implicit val eventDeserializer = new CreatedOrderDeserializer

  val duration = Duration.ofSeconds(5)

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "1")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  "Event listener" should {
    "receive the event to which it's registered" in {

      withRunningKafka {

        sendCreateOrderEventToTopic()

        val consumer = new KafkaConsumer[String, CreatedOrder](props, new StringDeserializer, new CreatedOrderDeserializer)
        val topics = new util.ArrayList[String]()
        topics.add("test-topic")
        consumer.subscribe(topics)

        val message: ConsumerRecords[String, CreatedOrder] = consumer.poll(duration)

        message.count() shouldBe 1

      }

    }

  }

  private def sendCreateOrderEventToTopic(): Unit = {
    val producer = aKafkaProducer[CreatedOrder]
    val event = CreatedOrder("1", "iPad")
    val producerRecord = new ProducerRecord[String, CreatedOrder]("test-topic", "event", event)
    producer.send(producerRecord).get()
    producer.close()
  }
}
