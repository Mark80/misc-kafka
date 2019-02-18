package kafka_stream.zmart.purchase

import java.time.LocalDateTime

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import kafka_stream.zmart.serdes.CommonFormat
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class Transaction(item: String, price: BigDecimal)

object Transaction extends DefaultJsonProtocol with SprayJsonSupport {

  implicit val transactionFormat = jsonFormat2(Transaction.apply)

}

case class PurchaseKey(customerId: String, date: LocalDateTime)

object PurchaseKey extends DefaultJsonProtocol with SprayJsonSupport with CommonFormat {

  implicit val purchaseKeyFormat: RootJsonFormat[PurchaseKey] = jsonFormat2(PurchaseKey.apply)

}

case class Purchase(firstName: String,
                    lastName: String,
                    customerId: String,
                    item: String,
                    zipCode: String,
                    date: LocalDateTime,
                    cardNumber: String,
                    department: String,
                    employeeId: String,
                    quantity: Int,
                    price: BigDecimal,
                    storeId: String)

object Purchase extends DefaultJsonProtocol with SprayJsonSupport with CommonFormat {

  implicit val purchaseFormat = jsonFormat12(Purchase.apply)

  def maskCreditCardNumber(purchase: Purchase): Purchase =
    purchase.copy(cardNumber = "XXXXXXX")

}

case class PurchasePattern(zipCode: String, items: String, amount: BigDecimal, dateTime: LocalDateTime)

object PurchasePattern extends DefaultJsonProtocol with SprayJsonSupport with CommonFormat {

  implicit val purchasePatternFormat = jsonFormat4(PurchasePattern.apply)

  def from(purchase: Purchase): PurchasePattern =
    PurchasePattern(purchase.zipCode, purchase.item, purchase.price * purchase.quantity, purchase.date)

}

case class RewardAccumulator(customerId: String,
                             purchaseTotal: BigDecimal,
                             totalRewardPoints: Int,
                             currentRewardPoints: Int,
                             daysFromLastPurchase: Int)

object RewardAccumulator extends DefaultJsonProtocol with SprayJsonSupport with CommonFormat {

  implicit val rewardAccumulatorFormat = jsonFormat5(RewardAccumulator.apply)

  def from(purchase: Purchase): RewardAccumulator =
    RewardAccumulator(
      customerId = purchase.customerId,
      purchaseTotal = purchase.price * purchase.quantity,
      totalRewardPoints = (purchase.price * purchase.quantity).toInt,
      currentRewardPoints = (purchase.price * purchase.quantity).toInt,
      daysFromLastPurchase = 0
    )

}

case class CorrelatedPurchase(customerId: String,
                              totalAmount: BigDecimal,
                              firstPurchaseTime: LocalDateTime,
                              secondPurchaseTime: LocalDateTime)

object CorrelatedPurchase extends DefaultJsonProtocol with SprayJsonSupport with CommonFormat {

  implicit val correlatePurchaseFormat = jsonFormat4(CorrelatedPurchase.apply)

}
