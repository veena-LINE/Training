Kafka Revision

Use Cases

	Sales
	Producers from order system
	click stream data from webpages/session
	recommendation system
	

WHY KAFKA
	real time analytics
	reduce low latency
	fault tolerance
	load balancing
	unified data pipeline

Kafka 
	core library - SCALA
	consumer/producer sdk - Java
	kafka connect - Java
	kafka sql - Java

List out few terminologies used in Kafka
Elements/Components of Kafka


Publisher/Susbcriber, Producer/Consumer
Kafka cluster, multiple brokers
			which accept messages
			store the messages
			make the data avaialble to consumer
			topics
			partitions - managed by partition
				
Consumer
	distributed
	Fault tolerance
	Poll data - back pressure
	
Topics
	paritions
	replications
	retention policy - default 7 days
					 - whether consumer reads message or not, th e will be deleted

Partitions
		???
		consumer can read messages from partition on specific offset
		replications, where copies the data is stored in multiple brokers
		
		
		0000x.log - data -- how big the file size can grow , 1 GB by default
					after reaching 1 GB size,
					it will create new log file
						00010011111111100.log
		0000x.timeindex - time to offset value
		0000x.index - offset values to data location in log file
		message always added at end of the persistent log

		Kafka partitions 
			store the messages
			no transformation
			deleting a data possible?? - NO
			Kafka messages are IMMUTABLE - NO DELETE, NO UPDATE
			
			
		Messgae stored in the partition  are ordered in that partition

Consumer Group
	Two consumers cannot read from same partitions
	Load Balance
	
	Consumer Reblancing - based on number of consumers instances, kafka allocate paritions to each consumer
	
	3 consumers trying to read from 2 paritions - not possible
		2 consumers can read each parition individually
		1 consumer can be idle
	
confluent platform
	confluent local start...
	

Topic management
	create topic with number of paritions
		
ZooKeeper 
		Centralized service cluster manager
		topic management
		centralized
		Meta data about topic
		Maintain broker heart beat, monitor down status
		Distributed configuration manager
		Cluster details
		
Messages
		logically grouped by topic name
		actually stored in partition
		Messages in bytes
		Each message get offset number in  partition
		Messages has key/value
		
Producer
	serialize message
	decide the partition
	how? based key
	if key is null, 
	if key is not null, there default partitioner - hash based partition hash(key) % num_partition
	custom partitioner
	
	Producer push/send messages to broker
	
	console producer has option to produce message with key value pair with seperator
	
Consumer
	deserializer
	Consumer poll the data from broker

Kafka
	processing, streaming data
	
	
Questions?

**1. E-commerce portal, click stream**

	1 M users
	
	Avg - each user visit 5 pages/5 product pages
	
	5 M page views, 9:00 AM to 5:00 PM, 80% users are here
	
	Web Server
		which producer the data whenever it see a traffic from user
		
		8 hrs, 4M messages
		
	Broker
		Per hour, it can handle 100K messages
		
	consumers
		per hour, they can process 50K records
		
	Scaling of brokers, consumers, partitions
	
	
	
