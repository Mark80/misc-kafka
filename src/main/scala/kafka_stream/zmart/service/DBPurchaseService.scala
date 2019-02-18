package kafka_stream.zmart.service

import java.time.LocalDateTime

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object DBPurchaseService {

  var db = Map.empty[LocalDateTime, (String, String)]

  def save(date: LocalDateTime, employeeId: String, item: String): Future[Unit] =
    Future {
      db = db + (date -> (employeeId, item))
    }

}
