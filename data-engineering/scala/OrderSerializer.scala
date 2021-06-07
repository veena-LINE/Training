package examples

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.Serializer

import java.nio.charset.StandardCharsets

class OrderSerializer[T] extends  Serializer[T] {
  val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  // this code shall convert Invoice object data into bytes
  // POJO [In memory] ==> JSON TEXT {"price": 100,..} ==> bytes [34,56,73]

  // serialize function called automatically by producer during producer.send
  override def serialize(topic: String, record: T): Array[Byte] = {
    val bytes: Array[Byte] = objectMapper.writeValueAsBytes(record)
    bytes //return bytes, this will send to kafka
  }
}
