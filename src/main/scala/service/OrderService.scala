package service

import eventGateway.EventPublisher
import service.model.{CreateOrderCommand, CreatedOrder}

import scala.concurrent.{ExecutionContext, Future}

class OrderService(eventPublisher: EventPublisher)(implicit executionContext: ExecutionContext) {

  def createOrder(createOrderCommand: CreateOrderCommand): Future[Unit] = Future {
    val createOrder = CreatedOrder("1", value = createOrderCommand.items.mkString)
    eventPublisher.publish(createOrder)
    ()
  }

}
