package kafka_stream.zmart.popHops

import org.apache.kafka.streams.processor.{AbstractProcessor, To}

class BeerPurchaseProcessor(domesticSalesNode: String, internationalSalesNode: String)
    extends AbstractProcessor[String, BeerPurchase] {

  def process(key: String, beerPurchase: BeerPurchase): Unit = {

    val transactionCurrency: Currency = beerPurchase.currency
    if (transactionCurrency != Currency.DOLLAR) {

      val internationalSaleAmount = beerPurchase.totalSale
      val dollarBeerPurchase = beerPurchase.copy(currency = Currency.DOLLAR,
                                                 totalSale =
                                                   convertToDollars(transactionCurrency, internationalSaleAmount))
      context.forward(key, dollarBeerPurchase, To.child(internationalSalesNode))
    } else context.forward(key, beerPurchase, To.child(domesticSalesNode))

  }

  private def convertToDollars(currentCurrency: Currency, amount: BigDecimal) =
    amount * currentCurrency.conversionRate

}
