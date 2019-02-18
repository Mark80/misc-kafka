package commandEntryPoint.model

case class CreateOrderRequest(customerId: Long, items: List[Long])
