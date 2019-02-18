package eventGateway

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import net.manub.embeddedkafka.{Consumers, EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.{Cluster, TopicPartition}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.StreamsBuilder
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import service.model.CreatedOrder

import collection.JavaConverters._
import scala.util.Random

class EventPublisherSpec
    extends WordSpec
    with Matchers
    with EmbeddedKafka
    with Consumers
    with BeforeAndAfterAll {

//  implicit val config = EmbeddedKafkaConfig(kafkaPort = 9092)
//
//  implicit val eventSerializer = new CreatedOrderSerializer
//
//  implicit val keyDeserializer = new StringDeserializer
//  implicit val eventDeserializer = new CreatedOrderDeserializer

  // val publisher = new EventPublisher()

  val duration = Duration.ofSeconds(0)

  "EventPublisher" should {

//    "publish order created" ignore {
//
//      withRunningKafka {
//        val event = CreatedOrder("1", "iPad")
//        publisher.publish(event)
//        consumeFirst \ MessageFrom[CreatedOrder]("test-topic") shouldBe event
//      }
//
//    }

    "generic producer" in {

      val executor = Executors.newFixedThreadPool(4)

      val props = new Properties()

      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "true")

      val producer = new KafkaProducer[String, String](props)

      val producerRecord = new ProducerRecord[String, String]("test-topic", "key", "value")

      producer
        .send(producerRecord,
              (metadata: RecordMetadata, exception: Exception) => println(metadata.timestamp()))
        .get()

      val propsConsumer = new Properties()
      propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, "1")
      propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringDeserializer")
      propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringDeserializer")
      propsConsumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      val consumer = new KafkaConsumer[String, String](propsConsumer)

      consumer.subscribe(util.Arrays.asList("test-topic"))
      val total = consumeMessage(consumer)

      total shouldBe 1000

    }

    "generic producer with retry" in {

      val executor = Executors.newFixedThreadPool(4)

      val props = new Properties()

      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.RETRIES_CONFIG, "2")
      props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

      val producer = new KafkaProducer[String, String](props)

      try {
        (1 to 2).foreach(_ =>
          executor.submit(new Runnable {
            override def run(): Unit = {
              val producerRecord =
                new ProducerRecord[String, String]("test-topic",
                                                   Random.nextInt().toString,
                                                   Random.alphanumeric.take(5).mkString)
              sendMessage(producer, producerRecord)
            }
          }))
      } finally {
        executor.awaitTermination(10000, TimeUnit.MILLISECONDS)
        executor.shutdown()
        producer.close()
      }
      val propsConsumer = new Properties()
      propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, "1")
      propsConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringDeserializer")
      propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringDeserializer")
      propsConsumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      val consumer = new KafkaConsumer[String, String](propsConsumer)

      consumer.subscribe(util.Arrays.asList("test-topic"))

      val total = consumeMessage(consumer)

      total shouldBe 2

    }

  }

  private def consumeMessage(consumer: KafkaConsumer[String, String]): Long = {
    var nonEmpty = true
    var total = 0
    try {
      while (nonEmpty) {

        val records = consumer.poll(100)
        consumer.commitAsync()
        total += records.count()

        nonEmpty = records.count() > 0

      }
    } finally {
      consumer.commitSync()
      consumer.close()
    }

    total

  }

  private def sendMessage(producer: KafkaProducer[String, String],
                          producerRecord: ProducerRecord[String, String]) =
    producer.send(
      producerRecord,
      (metadata: RecordMetadata, ex: Exception) => {
        if (metadata != null) {
          println(metadata.offset())
          println(metadata.topic())
        } else
          ex.printStackTrace()

      }
    )

  override def afterAll(): Unit = ()
  //publisher.close()

}

class CustomPartitioner extends Partitioner {

  override def partition(topic: String,
                         key: scala.Any,
                         keyBytes: Array[Byte],
                         value: scala.Any,
                         valueBytes: Array[Byte],
                         cluster: Cluster): Int = 0

  override def close(): Unit = ()

  override def configure(configs: util.Map[String, _]): Unit = ()
}
