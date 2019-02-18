package kafka_stream.zmart.stock

import java.time.LocalDateTime

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import kafka_stream.zmart.serdes.CommonFormat
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class Stock(symbol: String,
                 sector: String,
                 industry: String,
                 shares: Int,
                 price: BigDecimal,
                 customerId: String,
                 date: LocalDateTime,
                 purchase: Boolean)

object Stock extends DefaultJsonProtocol with SprayJsonSupport with CommonFormat {

  implicit val stockFormat = jsonFormat8(Stock.apply)

}

case class ShareVolume(symbol: String, shares: Int, industry: String)

object ShareVolume extends DefaultJsonProtocol with SprayJsonSupport with CommonFormat {

  def from(stock: Stock): ShareVolume =
    ShareVolume(stock.symbol, stock.shares, stock.industry)

  implicit val shareVolumeFormat: RootJsonFormat[ShareVolume] = jsonFormat3(ShareVolume.apply)

}

case class TransactionSummary(customerId: String,
                              symbol: String,
                              industry: String,
                              count: Long = 0,
                              customerName: String,
                              companyName: String)

object TransactionSummary extends DefaultJsonProtocol with SprayJsonSupport with CommonFormat {

  def from(stock: Stock): TransactionSummary =
    TransactionSummary(stock.customerId, stock.symbol, stock.industry, 0, "", "")

  implicit val transactionSummaryFormat: RootJsonFormat[TransactionSummary] = jsonFormat6(TransactionSummary.apply)

}

case class FinancialNews(industry: String, news: String)

object FinancialNews extends DefaultJsonProtocol with SprayJsonSupport with CommonFormat {

  implicit val financialNewsFormat: RootJsonFormat[FinancialNews] = jsonFormat2(FinancialNews.apply)

}
