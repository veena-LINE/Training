package examples

import java.util
import scala.collection.JavaConverters._

object StateAmountConsumer extends App {
  import org.apache.kafka.clients.consumer.KafkaConsumer

  import java.util.Properties

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  // deserializer
  // kafka broker has bytes
  // consumer should convert the bytes to string format
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  // custom deserializer converting json bytes to StateAmount object
  props.put("value.deserializer", "examples.StateAmountDeserializer")
  props.put("group.id", "state-amount-consumer")

  //[String, StateAmount] means key is String type, Value is StateAmount  type
  val consumer = new KafkaConsumer[String, StateAmount](props)

  val TOPIC = "statewise-amount"
  consumer.subscribe( util.Collections.singletonList(TOPIC))

  while (true) {
    // consumer poll data from broker
    // poll function internally calls deserialize function
    // that converts bytes to invoice
     val records = consumer.poll(500)

    for (record <- records.asScala) {
      val partition = record.partition()
      val offset = record.offset()
      val key = record.key()

      // object type, by deserializer
      val value: StateAmount = record.value()
      println("amount " + value.amount)
       println("Invoice Obj " + value)
      println("Got message Partition " + partition + ", offset  " + offset  + " " + key + ":" + value)
    }
  }
}
