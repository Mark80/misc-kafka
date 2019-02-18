package commandEntryPoint

import java.time.Duration
import java.util
import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import eventGateway.{CreatedOrderDeserializer, CreatedOrderSerializer, EventPublisher}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import service.OrderService
import service.model.CreatedOrder

object App {

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "1")

  implicit val eventSerializer = new CreatedOrderSerializer
  implicit val system = ActorSystem("my-system")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  def main(args: Array[String]): Unit = {

    val orderService = new OrderService(new EventPublisher())

    val entryPoint = new OrderEntryPoint(orderService)

    Http().bindAndHandle(entryPoint.routes, "localhost", 8080)

    val consumer = new KafkaConsumer[String, CreatedOrder](props, new StringDeserializer, new CreatedOrderDeserializer)
    val topics = new util.ArrayList[String]()
    topics.add("test-topic")
    consumer.subscribe(topics)

    println("Server online at http://localhost:8080/")

    new Thread() {
      override def run(): Unit =
        while (true) {
          val records = consumer.poll(Duration.ofSeconds(1))
          records.forEach { record =>
            println(record.value())
          }

        }

    }.start()

  }

}
