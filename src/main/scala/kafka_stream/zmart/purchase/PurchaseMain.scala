package kafka_stream.zmart.purchase

import java.time.LocalDateTime
import java.util.concurrent.CountDownLatch

import kafka_stream.zmart.producer.PurchaseProducer
import kafka_stream.zmart.service.DBPurchaseService

import scala.concurrent.ExecutionContext.Implicits.global

object PurchaseMain {

  def main(args: Array[String]): Unit = {

    val purchaseProducer = new PurchaseProducer

    val latch = new CountDownLatch(1)

    val purchaseCoffee = Purchase(
      customerId = "id",
      price = 1000,
      item = "book",
      zipCode = "zipCode",
      date = LocalDateTime.now(),
      cardNumber = "card number",
      employeeId = "emploeeId",
      firstName = "Name",
      lastName = "LastName",
      department = "Coffee",
      quantity = 2,
      storeId = "storeId"
    )

    val purchaseElectronics = Purchase(
      customerId = "id3",
      price = 1000,
      item = "book",
      zipCode = "zipCode",
      date = LocalDateTime.now(),
      cardNumber = "card number",
      employeeId = "emploeeId",
      firstName = "Name",
      lastName = "LastName",
      department = "Electronics",
      quantity = 2,
      storeId = "storeId"
    )

    val froudPurchase = Purchase(
      customerId = "id4",
      price = 1000,
      item = "book",
      zipCode = "zipCode",
      date = LocalDateTime.now(),
      cardNumber = "card number",
      employeeId = "0000",
      firstName = "Name",
      lastName = "LastName",
      department = "Electronics",
      quantity = 2,
      storeId = "storeId"
    )

    val waste = Purchase(
      customerId = "id5",
      price = 10,
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

    val joinableElectronics = Purchase(
      customerId = "id2",
      price = 1000,
      item = "book",
      zipCode = "zipCode",
      date = LocalDateTime.now().minusSeconds(1),
      cardNumber = "card number",
      employeeId = "emploeeId",
      firstName = "Name",
      lastName = "LastName",
      department = "Electronics",
      quantity = 2,
      storeId = "storeId"
    )

    val joinableCoffee = Purchase(
      customerId = "id2",
      price = 100,
      item = "book",
      zipCode = "zipCode",
      date = LocalDateTime.now(),
      cardNumber = "card number",
      employeeId = "emploeeId",
      firstName = "Name",
      lastName = "LastName",
      department = "Coffee",
      quantity = 2,
      storeId = "storeId"
    )

    for {
      _ <- purchaseProducer.send(purchaseCoffee)
      _ <- purchaseProducer.send(purchaseCoffee)
      _ <- purchaseProducer.send(purchaseElectronics)
      _ <- purchaseProducer.send(purchaseElectronics.copy(price = 2000))
      _ <- purchaseProducer.send(waste)
      _ <- purchaseProducer.send(waste)
      _ <- purchaseProducer.send(froudPurchase)
      _ <- purchaseProducer.send(joinableCoffee)
      _ <- purchaseProducer.send(joinableElectronics)
    } yield ()

    val (closeStream, getPointsByCustomerId) = (new PurchaseStream).start()

    sys.addShutdownHook({
      println("shooting down")
      closeStream()
      purchaseProducer.close()
      latch.countDown()
    })

    Thread.sleep(5500)

    println(getPointsByCustomerId("id"))
    println(DBPurchaseService.db)

    latch.await()

  }

}
