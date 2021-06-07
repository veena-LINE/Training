package examples

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.kafka.common.serialization.Deserializer


class StateAmountDeserializer extends  Deserializer[StateAmount] {
  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

  // convert bytes to  StateAmount object
  // when consumer calls poll method, this method is invoked automatically

  override def deserialize(topic: String, bytes: Array[Byte]): StateAmount = {
    val stateAmount: StateAmount = objectMapper.readValue[StateAmount] (bytes)
    stateAmount // return the object to consumer
  }
}
