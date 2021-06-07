package examples

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
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  //[String, String] means key is String type, Value also string type
  val producer = new KafkaProducer[String, String](props)

  val TOPIC = "orders";

  for (i <- 1 to 1000) {
    val key = "Order" + i // Order1, Order2...
    val value =   (i * 100 ).toString() // 100, 200,......
    val record = new ProducerRecord(TOPIC, key, value)
    println("Writing " + key + ":" + value)
    producer.send(record)
    Thread.sleep(5000)
  }

  producer.close()
}
