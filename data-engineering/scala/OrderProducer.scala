package examples

import scala.util.Random

object OrderProducer extends  App {
  // comments
  import java.util.Properties // producer settings
  // _ means import all
  import org.apache.kafka.clients.producer._

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  // kafka only knows bytes
  // producer to convert key to bytes, values to bytes
  // producer will have serializer
  // the key which is string type to bytes
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  // examples is package name, OrderSerializer is a class name
  props.put("value.serializer", "examples.OrderSerializer")

  //[String, Order] means key is String type, Value  is Invoice type
  // value is converted to bytes by OrderSerializer
  val producer = new KafkaProducer[String, Order](props)

  val TOPIC = "orders";

  val random: Random = new Random()

  val state_codes = List("NY", "CO", "TN", "OH", "OK")

  for (i <- 1 to 1000) {
     val state = state_codes(random.nextInt(state_codes.size))
    val qty = 1 + random.nextInt(20) // max 20, min 1
    val price = 1 + random.nextInt(20) // max 20, min 1

    val order = Order(state, qty, price) // value
    // using state as key
     val record = new ProducerRecord(TOPIC, state, order)
    println("Writing " + state + ":" + order)
    // this will call OrderSerializer.serialize to serialize order POJ/Memory into json bytes
    producer.send(record)
    Thread.sleep(5000)
  }

  producer.close()
}
