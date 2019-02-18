package commandEntryPoint.model

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol

trait JsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {

  implicit val createOrderProtocol = jsonFormat2(CreateOrderRequest)

}
