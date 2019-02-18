package kafka_stream.zmart.purchase

import java.util.Properties

import kafka_stream.zmart._
import kafka_stream.zmart.serdes._
import kafka_stream.zmart.service.DBPurchaseService
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.state.{QueryableStoreTypes, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

class PurchaseStream {

  private val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-1")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    p.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
          classOf[TransactionTimestampExtractor])
    p
  }

  implicit val keySerde: Serde[PurchaseKey] = SerdeFactory.apply[PurchaseKey]
  implicit val purchaseSerde: Serde[Purchase] = SerdeFactory.apply[Purchase]
  implicit val purchasePatternSerde: Serde[PurchasePattern] = SerdeFactory.apply[PurchasePattern]
  implicit val rewardSerde: Serde[RewardAccumulator] = SerdeFactory.apply[RewardAccumulator]
  implicit val correlatedPurchaseSerde: Serde[CorrelatedPurchase] =
    SerdeFactory.apply[CorrelatedPurchase]

  implicit val consumed = Consumed.`with`(keySerde, purchaseSerde)
  implicit val producedPurchase = Produced.`with`(keySerde, purchaseSerde)
  implicit val producedPurchasePattern = Produced.`with`(keySerde, purchasePatternSerde)
  implicit val rewardProduced = Produced.`with`(keySerde, rewardSerde)

  implicit val producedBranch = Produced.`with`(Serdes.String, purchaseSerde)
  implicit val joinedPurchase = Joined.`with`(Serdes.String, purchaseSerde, purchaseSerde)
  implicit val producedCorrelatedPurchase = Produced.`with`(Serdes.String, correlatedPurchaseSerde)

  private val timeWindow = 5 * 5000

  def start(): (() => Unit, String => Int) = {
    val builder = new StreamsBuilder

    val purchaseWithMaskedCreditCardNumber: kstream.KStream[PurchaseKey, Purchase] = builder
      .stream("purchase")
      .filter((_, purchase) => (purchase.price * purchase.quantity) > 100)
      .mapValues((_, purchase) => Purchase.maskCreditCardNumber(purchase))

    purchaseWithMaskedCreditCardNumber
      .filter((_, purchase) => purchase.employeeId == "0000")
      .foreach((_, purchase) =>
        DBPurchaseService.save(purchase.date, purchase.employeeId, purchase.item))

    val Array(coffeeStream, electronicsStream) =
      purchaseWithMaskedCreditCardNumber
        .selectKey((purchaseKey, _) => purchaseKey.customerId)
        .branch(isCoffee, isElectronics)

    coffeeStream.to("coffee")
    electronicsStream.to("electronics")

    val joinedStream: kstream.KStream[String, CorrelatedPurchase] =
      coffeeStream.join(electronicsStream)(purchaseJoiner, JoinWindows.of(timeWindow))

    joinedStream.to("joinedStream")

    purchaseWithMaskedCreditCardNumber
      .mapValues((_, purchase) => PurchasePattern.from(purchase))
      .to("patterns")

    val rewardsStateStoreName = "rewardTable"

    val storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName)
    val storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String, Serdes.Integer)

    builder.addStateStore(storeBuilder)

    val storeTransformer: PurchaseRewardTransformer = new PurchaseRewardTransformer(
      rewardsStateStoreName)

    val stateFullReward = purchaseWithMaskedCreditCardNumber
      .transformValues(new Supplier(rewardsStateStoreName, storeTransformer), rewardsStateStoreName)

    stateFullReward.to("reward")

    purchaseWithMaskedCreditCardNumber.to("purchase-masked")

    val stream = new KafkaStreams(builder.build(), props)

    stream.start()

    (() => stream.close(), (customerId: String) => storeTransformer.get(customerId))

  }

  val isElectronics = (_: String, purchase: Purchase) => purchase.department == "Electronics"
  val isCoffee = (_: String, purchase: Purchase) => purchase.department == "Coffee"

  def purchaseJoiner(value1: Purchase, value2: Purchase): CorrelatedPurchase = {
    val totalAmount = value1.price * value1.quantity + value2.price * value2.quantity

    CorrelatedPurchase(
      value1.customerId,
      totalAmount,
      value1.date,
      value2.date
    )
  }

}

class Supplier(storeName: String, transformer: PurchaseRewardTransformer)
    extends ValueTransformerSupplier[Purchase, RewardAccumulator] {

  def get(): ValueTransformer[Purchase, RewardAccumulator] = transformer
}
