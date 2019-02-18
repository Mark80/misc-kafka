package zmart

import java.time.LocalDateTime

import kafka_stream.zmart.producer.TransactionProducer
import kafka_stream.zmart.serdes.{DeserializerFactory, SerializerFactory}
import kafka_stream.zmart.purchase.{Purchase, Transaction}
import net.manub.embeddedkafka.{Consumers, EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.ExecutionContext.Implicits.global

class TransactionProducerSpec
    extends WordSpec
    with Matchers
    with EmbeddedKafka
    with Consumers
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience {

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092)
  implicit val transactionSerializer = SerializerFactory[Transaction]
  implicit val transactionDeserializer = DeserializerFactory[Transaction]

  val transactionProducer = new TransactionProducer()

  override def afterAll(): Unit =
    transactionProducer.close()

  "TransactionProducer" should {

    "public transaction to kafka topic" in {

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

        transactionProducer
          .send(purchase)
          .futureValue
        consumeFirstMessageFrom[Transaction]("transaction") shouldBe Transaction("id", 1000)

      }

    }

  }

}
