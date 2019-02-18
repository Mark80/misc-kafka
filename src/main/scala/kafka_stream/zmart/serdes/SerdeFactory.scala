package kafka_stream.zmart.serdes

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import spray.json.JsonFormat

object SerdeFactory {

  def apply[T](implicit format: JsonFormat[T]): Serde[T] = new Serde[T] {
    def deserializer(): Deserializer[T] = DeserializerFactory[T]
    def serializer(): Serializer[T] = SerializerFactory[T]
    def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
    def close(): Unit = ()
  }

}
