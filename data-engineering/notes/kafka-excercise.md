
Time: 10 mins

1. Create a topic called orders with partitions 4 paritions, 1 replica
2. console-producer with key and value delimited by :, format orderId:json  example,  1234:10 where is 1234 is order id, 10 is the value
3. console-consumer, with group id called order-terminal-consumer-group
4. Run 4 instances of console-consumer mentioned on step 3

-----


Time: 15 Mins

Scala Code

1. Write a Scala producer for order topic
2. Write a scala consumer for order topic


Copy/clone SimpleProducer.scala as OrderProducer.scala, change the topic, change the key to include Order1, Order2, Order3, value also i * 100, 100, 200, 300, 400

 In scala to convert int to string, 10.toString()
 
 Copy/clone the SimpleConsumer.scala as OrderConsumer.scala, then change the topic, susbcribe for message, print...
      set the group.id property to "scala-order-consumer"
