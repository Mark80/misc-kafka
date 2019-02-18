package kafka_stream.zmart.popHops

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import kafka_stream.zmart.serdes.CommonFormat
import spray.json.{DefaultJsonProtocol, JsNumber, JsValue, RootJsonFormat}

trait Currency {

  def conversionRate: BigDecimal

  override def equals(obj: scala.Any): Boolean = {
    val currency = obj.asInstanceOf[Currency]
    conversionRate == currency.conversionRate
  }

}

object Currency {

  val DOLLAR = new Currency {
    val conversionRate: BigDecimal = 1.0
  }

  val EURO = new Currency {
    val conversionRate: BigDecimal = 1.9
  }

  val POUNDS = new Currency {
    val conversionRate: BigDecimal = 1.9
  }

  implicit val currencyFormat = new RootJsonFormat[Currency] {

    def write(currency: Currency): JsValue =
      new JsNumber(currency.conversionRate)

    def read(json: JsValue): Currency = json match {
      case JsNumber(value) =>
        new Currency {
          val conversionRate: BigDecimal = value
        }

    }
  }

}

case class BeerPurchase(currency: Currency,
                        totalSale: BigDecimal,
                        numberCases: Int,
                        beeType: String)

object BeerPurchase extends DefaultJsonProtocol with SprayJsonSupport with CommonFormat {

  implicit val beerPurchaseFormat = jsonFormat4(BeerPurchase.apply)

}
