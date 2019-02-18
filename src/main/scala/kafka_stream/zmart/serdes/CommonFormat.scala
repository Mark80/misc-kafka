package kafka_stream.zmart.serdes

import java.time.LocalDateTime

import spray.json.{JsString, JsValue, JsonFormat}

trait CommonFormat {

  implicit val dateTimeFormat = new JsonFormat[LocalDateTime] {

    def write(dateTime: LocalDateTime): JsValue =
      new JsString(dateTime.toString)

    def read(json: JsValue): LocalDateTime =
      json match {
        case JsString(value) =>
          LocalDateTime.parse(value)

        case _ => throw new RuntimeException("date parsing error")
      }
  }

}
