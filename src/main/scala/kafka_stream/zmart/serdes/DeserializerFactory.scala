package kafka_stream.zmart.serdes

import java.util

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.kafka.common.serialization.Deserializer
import spray.json.{DefaultJsonProtocol, JsonFormat, JsonParser}

object DeserializerFactory {

  def apply[T](implicit format: JsonFormat[T]): Deserializer[T] =
    new Deserializer[T] with DefaultJsonProtocol with SprayJsonSupport {

      def close(): Unit = ()

      def deserialize(topic: String, data: Array[Byte]): T =
        JsonParser(new String(data)).convertTo[T]

      def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
    }

}
