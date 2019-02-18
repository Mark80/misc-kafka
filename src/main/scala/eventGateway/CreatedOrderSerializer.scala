package eventGateway

import java.util

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import org.apache.kafka.common.serialization.Serializer
import service.model.CreatedOrder
import spray.json.DefaultJsonProtocol

class CreatedOrderSerializer extends Serializer[CreatedOrder] with DefaultJsonProtocol with SprayJsonSupport {

  import spray.json._

  implicit val eventFormat = jsonFormat2(CreatedOrder)

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, data: CreatedOrder): Array[Byte] =
    data.toJson.toString().getBytes

  override def close(): Unit = ()

}
