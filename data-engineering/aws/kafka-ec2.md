
sudo apt update 

sudo apt upgrade 


sudo apt install openjdk-8-jre-headless

wget https://archive.apache.org/dist/kafka/2.3.0/kafka_2.11-2.3.0.tgz


tar -xzf kafka_2.11-2.3.0.tgz

cd kafka_2.11-2.3.0


you are running this command on 'ubuntu@ip-172-31-39-83:~/kafka_2.11-2.3.0$'
```


bin/kafka-topics.sh --create --zookeeper  hostname:2181 --replication-factor 1 --partitions 4 --topic greetings

 
bin/kafka-topics.sh --describe --zookeeper hostname:2181 --topic greetings



```

```


run this command on one ssh, on directory ubuntu@ip-172-31-39-83:~/kafka_2.11-2.3.0

bin/kafka-console-producer.sh --broker-list host1:9092,host2:9092 --topic greetings

run on new terminal and run on second ssh on directory ubuntu@ip-172-31-39-83:~/kafka_2.11-2.3.0

bin/kafka-console-consumer.sh --bootstrap-server host1:9092,host-2.amazonaws.com:9092 --topic greetings --from-beginning
```
