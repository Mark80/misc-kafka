package eventGateway

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import service.model.CreatedOrder

class EventPublisher(implicit eventSerializer: CreatedOrderSerializer) {

  val producerConf = new Properties()
  producerConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val producer = new KafkaProducer[String, CreatedOrder](producerConf, new StringSerializer, eventSerializer)

  def publish(event: CreatedOrder): RecordMetadata = {
    val producerRecord = new ProducerRecord[String, CreatedOrder]("test-topic", "event", event)
    val metadata = producer.send(producerRecord).get()
    metadata
  }

  def close() = producer.close()

}
