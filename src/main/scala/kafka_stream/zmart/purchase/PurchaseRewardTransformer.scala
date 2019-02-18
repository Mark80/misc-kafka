package kafka_stream.zmart.purchase

import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

class PurchaseRewardTransformer(storeName: String) extends ValueTransformer[Purchase, RewardAccumulator] {

  var store: KeyValueStore[String, Int] = _

  def init(context: ProcessorContext): Unit =
    store = context.getStateStore(storeName).asInstanceOf[KeyValueStore[String, Int]]

  def transform(purchase: Purchase): RewardAccumulator =
    Option(store.get(purchase.customerId)) match {

      case Some(points) =>
        val accumulator = RewardAccumulator.from(purchase)
        val newTotalPoints = points + accumulator.totalRewardPoints
        store.put(purchase.customerId, newTotalPoints)
        accumulator.copy(totalRewardPoints = newTotalPoints)

      case None =>
        val accumulator = RewardAccumulator.from(purchase)
        store.put(purchase.customerId, accumulator.totalRewardPoints)
        accumulator

    }

  def get(customerId: String): Int =
    store.get(customerId)

  def close(): Unit = ()
}
