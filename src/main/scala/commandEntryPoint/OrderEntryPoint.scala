package commandEntryPoint

import akka.http.scaladsl.server._
import Directives._
import akka.http.scaladsl.model.StatusCodes
import commandEntryPoint.model.{CreateOrderRequest, JsonProtocol}
import service.OrderService
import service.model.CreateOrderCommand

import scala.concurrent.ExecutionContext

class OrderEntryPoint(orderService: OrderService)(implicit executionContext: ExecutionContext) extends JsonProtocol {

  def createOrder: Route = path("order") {
    post {
      entity(as[CreateOrderRequest]) { request =>
        complete {
          val command = CreateOrderCommand(request.customerId, request.items)
          orderService.createOrder(command).map(_ => StatusCodes.Created)
        }
      }
    }
  }

  val routes = createOrder

}
