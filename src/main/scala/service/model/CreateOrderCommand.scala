package service.model

case class CreateOrderCommand(customerId: Long, items: List[Long])
