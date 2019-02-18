package kafka_stream.zmart.serdes

import java.util

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.kafka.common.serialization.Serializer

object SerializerFactory {

  import spray.json._

  def apply[T](implicit format: JsonFormat[T]) = new Serializer[T] with DefaultJsonProtocol with SprayJsonSupport {

    def serialize(topic: String, data: T): Array[Byte] =
      data.toJson.toString().getBytes

    def close(): Unit = ()

    def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  }

}
