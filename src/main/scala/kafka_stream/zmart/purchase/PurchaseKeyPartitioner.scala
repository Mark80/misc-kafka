package kafka_stream.zmart.purchase

import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.Cluster

class PurchaseKeyPartitioner extends DefaultPartitioner {

  override def partition(topic: String,
                         key: scala.Any,
                         keyBytes: Array[Byte],
                         value: scala.Any,
                         valueBytes: Array[Byte],
                         cluster: Cluster): Int =
    key match {
      case PurchaseKey(customerId, _) =>
        super.partition(topic, customerId, customerId.getBytes, value, valueBytes, cluster)

      case _ => super.partition(topic, key, keyBytes, value, valueBytes, cluster)
    }

}
