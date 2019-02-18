package kafka_stream.zmart.purchase

import java.time.ZoneId

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

class TransactionTimestampExtractor extends TimestampExtractor {

  def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {
    val purchase = record.value().asInstanceOf[Purchase]
    purchase.date.atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
  }

}
