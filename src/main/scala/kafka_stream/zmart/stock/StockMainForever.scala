package kafka_stream.zmart.stock

import java.time.LocalDateTime
import java.util.concurrent.CountDownLatch

import kafka_stream.zmart.producer.{CustomerProducer, NewsProducer, StockProducer}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

object StockMainForever {

  def main(args: Array[String]): Unit = {

    val stockProducer = new StockProducer()

    Thread.sleep(500)

    val stock1 = Stock(symbol = "ABC",
                       sector = "sector",
                       industry = "industry",
                       shares = 5,
                       price = 1200,
                       customerId = "id",
                       date = LocalDateTime.now(),
                       purchase = false)

    val stock2 = Stock(symbol = "CBA",
                       sector = "sector",
                       industry = "industry",
                       shares = 5,
                       price = 405,
                       customerId = "id",
                       date = LocalDateTime.now(),
                       purchase = false)

    val stock3 = Stock(symbol = "XZV",
                       sector = "sector",
                       industry = "industry",
                       shares = 5,
                       price = 1500,
                       customerId = "id",
                       date = LocalDateTime.now(),
                       purchase = false)

    val stock4 = Stock(symbol = "XZVAA",
                       sector = "sector",
                       industry = "industry",
                       shares = 1,
                       price = 1500,
                       customerId = "id",
                       date = LocalDateTime.now(),
                       purchase = false)

    val stock5 = Stock(symbol = "XZVBB",
                       sector = "sector",
                       industry = "industry2",
                       shares = 2,
                       price = 1500,
                       customerId = "id2",
                       date = LocalDateTime.now(),
                       purchase = false)

    val stock6 = Stock(symbol = "XZVCC",
                       sector = "sector",
                       industry = "industry2",
                       shares = 5,
                       price = 1500,
                       customerId = "id2",
                       date = LocalDateTime.now(),
                       purchase = false)

    val latch = new CountDownLatch(1)

    val producer = new Thread() {

      import scala.concurrent.duration._

      override def run(): Unit =
        while (latch.getCount > 0) {
          Thread.sleep(2000)
          Await.ready(
            for {
              _ <- stockProducer.send(stock1)
              _ <- stockProducer.send(stock2)
              _ <- stockProducer.send(stock2)
              _ <- stockProducer.send(stock3)
              _ <- stockProducer.send(stock3)
              _ <- stockProducer.send(stock3)
              _ <- stockProducer.send(stock3)
              _ <- stockProducer.send(stock3)
              _ <- stockProducer.send(stock4)
              _ <- stockProducer.send(stock5)
              _ <- stockProducer.send(stock6)
              _ <- stockProducer.send(stock1.copy(price = 1300))
              _ <- stockProducer.send(stock2.copy(price = 300))
              _ <- stockProducer.send(stock1.copy(price = 2000))
              _ <- stockProducer.send(stock2.copy(price = 700))
            } yield (),
            1000 seconds
          )
        }
    }

    producer.start()

    sys.addShutdownHook({
      println("shooting down")
      latch.countDown()
      stockProducer.close()
    })

    latch.await()

  }

}
