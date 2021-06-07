 - ZooKeeper - 1 instance [not in cluster]
 - Kafka Broker -  PORT 9092, directory where the data is stored, directory to the files

 4 instances of kafka
  They run different machines on port 9092. Since we have 1 VM, we will 4 Brokers in 1 VM in cluster
  
  WE will use different ports for cluster purpose, different directory for cluster purpose.
  
  Each broker, needs unique id, numeric type, integer, 0, 1, 2...............64 bit number
  Port number: 9092, 9093, 9094, 9095
  Directories, use /tmp directory to store the content of the kafka data
  
  We will not use confluent local command.....
  
  
  
  
 ```
 confluent local stop
 ```
 
 ```
 cd $KAFKA_HOME
 
 ```
 
 
 ## Setting up Zoo Keeper in Cluster
  ZooKeeper runs on port 2181
  
  Open new terminal, paste below command to start zooKeeper
  
```  
  $KAFKA_HOME/bin/zookeeper-server-start $KAFKA_HOME/etc/kafka/zookeeper.properties
```
 
 ## Setting up Brokers in Cluster
 
 server.properties - broker setting file

few propertis to be overridden

```
broker.id=0
listeners=PLAINTEXT://:9092
log.dirs=/tmp/kafka-logs
```  
  
  
Starting Broker 0 on Port 9092, which store content on /tmp/kafka-logs-0
 
Open new terminal 

Broker 0 setting

```
$KAFKA_HOME/bin/kafka-server-start $KAFKA_HOME/etc/kafka/server.properties --override broker.id=0  --override listeners=PLAINTEXT://:9092 --override log.dirs=/tmp/kafka-logs-0 --override confluent.metadata.server.listeners=http://0.0.0.0:8090 --override confluent.metadata.server.advertised.listeners=http://127.0.0.1:8090
```

Open new terminal 

Broker 1 setting

```
$KAFKA_HOME/bin/kafka-server-start $KAFKA_HOME/etc/kafka/server.properties --override broker.id=1  --override listeners=PLAINTEXT://:9093 --override log.dirs=/tmp/kafka-logs-1 --override confluent.metadata.server.listeners=http://0.0.0.0:8091 --override confluent.metadata.server.advertised.listeners=http://127.0.0.1:8091
```

 
Open new terminal 

Broker 2 setting

```
$KAFKA_HOME/bin/kafka-server-start $KAFKA_HOME/etc/kafka/server.properties --override broker.id=2  --override listeners=PLAINTEXT://:9094 --override log.dirs=/tmp/kafka-logs-2 --override confluent.metadata.server.listeners=http://0.0.0.0:8092 --override confluent.metadata.server.advertised.listeners=http://127.0.0.1:8092
```


Open new terminal 

Broker 3 setting

```
$KAFKA_HOME/bin/kafka-server-start $KAFKA_HOME/etc/kafka/server.properties --override broker.id=3  --override listeners=PLAINTEXT://:9095 --override log.dirs=/tmp/kafka-logs-3 --override confluent.metadata.server.listeners=http://0.0.0.0:8093 --override confluent.metadata.server.advertised.listeners=http://127.0.0.1:8093
```



open new terminal

zookeeper-shell, cli for zookeeper for debugging purpose..

```
zookeeper-shell localhost:2181
```

once it is connected, you can below commands

```

ls /

ls /brokers

ls /brokers/ids

ls /brokers/topics



```


open new terminal

#### Replication factor

**does not help on scaling/performance**, help on fault tolerance, if one broker fails, other copies is readily availble.

test replication factors, 

replication factor should less than or equal to  number of brokers

10 brokers,  3 or 5 replications
20 brokers, 3 or 5 replications


```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 3 --partitions 4 --topic greetings

kafka-topics --describe --zookeeper localhost:2181 --topic greetings
```


Notes

For every partition, there shall be 1 leader will be there. Leader is a broker. 

1. Leader is responsible to accept kafka messsages from producer, write to its log file, 
2. Leader also responsible to server the consumers
3. Leader also responsible to sync the data to replicas


replicas - basically the brokers which replicate the parition data, replicas not participating with producer or with consumer. Used for replicas

ISR - In-sync-Replica - the list of brokers who has exact copy of leader data already synced
       if any broker fails between, the leader for the parition is choosen from In sync replicas, there shall be election co-orderinated by zookeeper to elect new leader for the parition, sub set of replicas
       
Offline: list of brokers which are offline [subset of replicas]
       



```
leader: brokerId
replicas: [brokerid1, brokerid2..]
isr - in-sync-replicas [broker id, broker2..]
offline: [broker id1, broker2]
````


kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic logs
kafka-topics --zookeeper localhost:2181 --alter --topic logs --partitions 3

### for replica settings partition reassignment-- explain in later session

kafka-topics --describe --zookeeper localhost:2181 --topic logs
