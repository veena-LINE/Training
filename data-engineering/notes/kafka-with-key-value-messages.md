
```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic greetings

```

```
kafka-topics --describe --zookeeper localhost:2181 --topic greetings
```


To know which partitions assigned to consumer instance?

```
kafka-consumer-groups --bootstrap-server localhost:9092 --list

kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group greetings-consumer-group

with active members if any

kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group greetings-consumer-group --members
```





```	
kafka-console-producer --broker-list localhost:9092 --topic greetings --property "parse.key=true" --property "key.separator=:"
```

>key:value
>NY:Happy New Year (Key is NY, seperator :), value part

example 

```
>IN:msg1
>USA:msg2
>CA:msg3
>UA:msg4
>DE:msg5
>FR:msg5
>USA:msg6
>AU:msg7
>SL:msg8
>CH:msg9
>TH:msg10
>SL:msg11
>TH:msg12
>
```

## Part 1

1 Consumer and 4 Partitions

every consumer shall have instance id, unique, prefixed with group name..

onsumer-greetings-consumer-group-1-9d6b969a-2def-426a-bbb7-fc6d24670b40
```
C1: [P0, P1, P2, P3] 
```

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings --group greetings-consumer-group --property print.key=true
```


## Part 2

open new terminal, run the consumer again, this add 2 consumer instance to the group greetings-consumer-group


consumer id: consumer-greetings-consumer-group-1-6fc2df59-cb9a-41b6-9244-ce2f5474828a

Kafka revoke all the partitiosn from C1 and 
rebalance partitions to C1 and C2

2 Consumer and 4 Partitions
```
C1:  [2, 3]
C2:  [0, 1]
```

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings --group greetings-consumer-group --property print.key=true
```


## Part 3

open new terminal, run the consumer again, this add 2 consumer instance to the group greetings-consumer-group


consumer id: consumer-greetings-consumer-group-1-0fd73a2b-42ef-4f4e-86a1-b9d1a85571be

Kafka revoke all the partitiosn from C1 and C2

rebalance partitions to C1 and C2, C3

3 Consumer and 4 Partitions
```
C1:  [ P3]
C2:  [ P2]
C3:  [P0, P1 ]
```

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings --group greetings-consumer-group --property print.key=true
```


## Part 4

open new terminal, run the consumer again, this add 2 consumer instance to the group greetings-consumer-group


consumer id:  consumer-greetings-consumer-group-1-fb2445ae-08a3-4001-9c98-feee2d59f34b


Kafka revoke all the partitiosn from C1 and C2, C3

rebalance partitions to C1 and C2, C3, C4

3 Consumer and 4 Partitions
```
C1:  [ 2]
C2:  [ 1]
C3:  [ 0]
C4:  [3]
```

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings --group greetings-consumer-group --property print.key=true
```

-------

## Part 5

Run 5th consumer on the same group, but we have only 4 partitions.

One consumer out of 5, shall be IDLED, MAY NOT BE THIS ONE

kafka revoke all the partitions again...

re-assign partitions to 4 of 5 consumers instances

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings --group greetings-consumer-group --property print.key=true
```


// Debug
to know the partition where the messages are stored
```
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings --partition 0  --from-beginning --property print.key=true
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings --partition 1  --from-beginning --property print.key=true
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings --partition 2  --from-beginning --property print.key=true
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings --partition 3  --from-beginning --property print.key=true

```

