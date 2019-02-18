package commandEntryPoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import commandEntryPoint.model.{CreateOrderRequest, JsonProtocol}
import org.scalatest.{Matchers, WordSpec}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.any
import org.scalatest.mockito.MockitoSugar
import service.OrderService
import service.model.CreateOrderCommand

import scala.concurrent.Future

class EntryPointSpec extends WordSpec with Matchers with ScalatestRouteTest with JsonProtocol with MockitoSugar {

  val orderServiceMock = mock[OrderService]
  val entryPoint = new OrderEntryPoint(orderServiceMock)

  "Endpoint" should {

    "receive a create a order command" in {

      val items = List(1L, 2L, 3L)
      val createOrder = CreateOrderRequest(customerId = 123L, items = items)

      val command = CreateOrderCommand(123, items)

      Post("/order", createOrder) ~> Route.seal(entryPoint.routes) ~> check {
        status shouldBe StatusCodes.Created
        verify(orderServiceMock).createOrder(command)
      }

    }

  }

}
