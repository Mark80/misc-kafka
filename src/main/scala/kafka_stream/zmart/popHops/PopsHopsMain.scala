package kafka_stream.zmart.popHops

import kafka_stream.zmart.serdes.{DeserializerFactory, SerializerFactory}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier, UsePreviousTimeOnInvalidTimestamp}

object PopsHopsMain {

  val purchaseSourceNodeName = "beer-purchase-source"
  val domesticSalesSink = "domestic-beer-sales"
  val internationalSalesSink = "international-beer-sales"
  val purchaseProcessor = "purchase-processor"

  val beerPurchaseSerializer = SerializerFactory[BeerPurchase]

  def main(args: Array[String]): Unit = {

    val supplier = new BeerSupplier(new BeerPurchaseProcessor(domesticSalesSink, internationalSalesSink))

    val topology = new Topology

    topology
      .addSource(
        AutoOffsetReset.LATEST,
        purchaseSourceNodeName,
        new UsePreviousTimeOnInvalidTimestamp(),
        new StringDeserializer,
        DeserializerFactory[BeerPurchase],
        "source"
      )
      .addProcessor(purchaseProcessor, supplier, purchaseSourceNodeName)
      .addSink(internationalSalesSink,
               "international-sales",
               new StringSerializer,
               beerPurchaseSerializer,
               purchaseProcessor)
      .addSink(domesticSalesSink, "domestic-sales", new StringSerializer, beerPurchaseSerializer, purchaseProcessor)

  }

}

class BeerSupplier(processor: BeerPurchaseProcessor) extends ProcessorSupplier[String, BeerPurchase] {
  def get(): Processor[String, BeerPurchase] = processor
}
