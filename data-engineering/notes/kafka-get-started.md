```
confluent local start

confluent local status
```

```
kafka-topics --list --zookeeper localhost:2181

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

kafka-topics --list --zookeeper localhost:2181

kafka-topics --describe --zookeeper localhost:2181 --topic test

```


Open a new terminal

```
kafka-console-producer --broker-list localhost:9092 --topic test
```


open new terminal 

listen for the messages published/latest

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic test
```

Ctrl + C to stop it

Produce messages when consumer down...

start consumer  with --from-beginging

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
```

### Excercise [5 mins]

create a topic called messsages with partition 1 and replication factor 1

run the producer in terminal and produce messsage to messsages topic

run the consumer and susbcribe from messages topics

and check when consumer offline, you could produce message

and try reading using from-begining


```

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic greetings

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions  3 --topic logs
```

```
kafka-topics --describe --zookeeper localhost:2181 --topic test

kafka-topics --describe --zookeeper localhost:2181 --topic greetings

kafka-topics --describe --zookeeper localhost:2181 --topic logs

```


```
 confluent local current 

```

note down the output directry name, where kafka store the configuration

```
 cd into directory shown fo local current command
 
 cd kafka
 cd data
 
 ls 
 
 we can see <<topic>>-<<partition> folders 

```



3 partitions, no key, partition is allocated, messages are stored in robin robin basic since no key avaialble.

```
kafka-console-producer --broker-list localhost:9092 --topic logs

Since no consumer group mentioned, below command, shall always get the data which produced now onwards, ie latest

kafka-console-consumer --bootstrap-server localhost:9092 --topic logs
kafka-console-consumer --bootstrap-server localhost:9092 --topic logs --from-beginning

kafka-console-consumer --bootstrap-server localhost:9092 --topic logs --partition 0  --from-beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic logs --partition 1  --from-beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic logs --partition 2  --from-beginning

from a specific partition, read msg from speicific offset

kafka-console-consumer --bootstrap-server localhost:9092 --topic logs --partition 0   --offset 3
kafka-console-consumer --bootstrap-server localhost:9092 --topic logs --partition 1   --offset 1
```


```
kafka-console-consumer --bootstrap-server localhost:9092 --topic logs  --group myconsumer-group1

produce some message and check in consumer

stop the consumer

produce more messages

and run the consumer with group id

kafka-console-consumer --bootstrap-server localhost:9092 --topic logs  --group myconsumer-group1


validate the consumer is not re-processing same message again and again

```



