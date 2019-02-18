package eventGateway

import java.util

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.kafka.common.serialization.Deserializer
import service.model.CreatedOrder
import spray.json.DefaultJsonProtocol

class CreatedOrderDeserializer extends Deserializer[CreatedOrder] with DefaultJsonProtocol with SprayJsonSupport {
  import spray.json._

  implicit val eventFormat = jsonFormat2(CreatedOrder)

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): CreatedOrder =
    JsonParser.apply(new String(data)).convertTo[CreatedOrder]

}
