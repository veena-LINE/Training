Day 8


Key to Partition

MAX PARITION - 3

Producer sending [{key: IN, value..}, {key: USA, value..}, {key: IN, value..}

Hashcode of (IN) - 103 % MAX PARTITION = 1
Hashcode of (USA) - 99 % MAX PARTITION = 0
Hashcode of (IN) - 103 % MAX PARTITION = 1

P0 - [  ] 
P1 - [ ]
P2 - []

---


MAX PARITION - 1

Orders [{key: IN, value..}, {key: USA, value..}, {key: IN, value..}

Hashcode of (IN) - 103 % MAX PARTITION = 0
Hashcode of (USA) - 99 % MAX PARTITION = 0
Hashcode of (IN) - 103 % MAX PARTITION = 0

P0 - [  {key: IN, value..}, {key: USA, value..}, {key: IN, value..}] 
 
 
 -----
 
 Consumer Group - order-processing
 
 Topic: orders
 
 Paritions: 3
 
 P0 - []
 P1 - []
 P2 - []
 
 
 
 -----
 Node1/Computer - 1
 
 Start a consumer with group id **order-processing**
 
 Kafka shall reblance partitions to consumer ratio..
 
 3 paritions, 1 consumer 
 
 kafka shall all partitions to 1 consumer 
 
 P0, P1, P2 ==> C1
 
 ----
after a while, traffic increases

 Node 2/Computer - 2 
 
  Start a consumer with group id **order-processing**
	 Kafka shall reblance partitions to consumer ratio..
	 
	Kafka shall revoke all the partitions from existing consumers
	 
	 Then assign partitions to consumer
	 
 3 paritions, 2 consumers 
 
 P0, P1 ==> C1
 P2     ==> C2
 
 ---
 
 after a while traffic increases 
 
  Node 3/Computer - 3
  
  Start a consumer with group id **order-processing**
  
  	Kafka shall revoke all the partitions from existing consumers

   Kafka shall reblance partitions to consumer ratio..
   
	 3 paritions, 3 consumers 
	 
	  P0 ==> C1
	  P1 ==> C2
 	  P3 ==> C3
	  
======
 Node 4/Computer - 4
  
  Start a consumer with group id **order-processing**
  	Kafka shall revoke all the partitions from existing consumers
   Kafka shall reblance partitions to consumer ratio..

	 3 paritions, 4 consumers 

	  P0 ==> C1
	  P1 ==> C4
 	  P3 ==> C3
	  
	  
	  C0 is IDLE
	  
==

Now C1 Crashes/you shut it down


  Start a consumer with group id **order-processing**
  	Kafka shall revoke all the partitions from existing consumers
   Kafka shall reblance partitions to consumer ratio..

	 3 paritions, 3 consumers 

 	  P0 ==> C0
	  P1 ==> C4
 	  P3 ==> C3
	  
---

Shutdown C4


  Start a consumer with group id **order-processing**
  	Kafka shall revoke all the partitions from existing consumers
   Kafka shall reblance partitions to consumer ratio..

	 3 paritions, 2 consumers 

 	  P0 ==> C0
	  P1 ==> C3
 	  P3 ==> C3

Shutdown C3


  Start a consumer with group id **order-processing**
  	Kafka shall revoke all the partitions from existing consumers
   Kafka shall reblance partitions to consumer ratio..

	 3 paritions, 1 consumers 

 	  P0 ==> C0
	  P1 ==> C0
 	  P3 ==> C0
	 
----

Orders 
	P0 [O0, O1, O2....N]
	P1 -(O11, O12..M)
	

**consumer group id "notification" / WRONG/Consumer** should do same job

	start a consumer that send email
		Send email for O0, O1,..N
	start another consuemer  that send sms/text
		Text (O11, O12..M)
		
**email-consumers**
	start consumer that only emails
	
""text-notification""
	start consumer that send only text messages
	
------------------

Python array 

arr[10] = [ "kafka", "spark".... ]
Index /Offset = 0 array[0] = Kafka

----

text.file../partition 

kafkfa...spark...consumer....

Offset = 0, 1, 2, 3.. 
Offset is a position where message can be address

00000000000.log 
	contains all the messages
	one message is bigger
	one can be smaller
	
	0000000000000hyellokafka........how are you
File location
	fseek
	ftell(14)
	fread
	
00000000000.index
	message to file location
	offset 0 - 0 file location
	offset 1 - 14 file location
	
00000000000.timeindex
	time the message written to offset index
	
	Feb 10, 2021 10:00:01 - offset 2000
	Feb 10, 2021 10:05 - offset 2001
	...
	Feb 11, 2021 10:05 - offset 3000
	
------

Spark - 32 paritions

Kafka -  4 paritions

Spark ==> Kafka

Messgaes
Tuple (key, value)  ==> 4 kafka partitions [p0, p1, p2, p3]

---


Why need more executors?
  parallel processing
  
P0 - [million of messages]

	spark 
			stream read from kafka
				executor 1 - consumer group  - orderprocessing
					P0 assigned, and P0 data is processed here
				
				executor 2 - consumer group - orderprocessing
					 IDLE, no partition left
				
					
-------



console-producer
	key + value
	
	based on key, the partition is decided
	

>hello kakfa (value only)
	
kafka-console-producer --broker-list localhost:9092 --topic greetings --property "parse.key=true" --property "key.separator=:"
	
>key:value
>NY:Happy New Year (Key is NY, seperator :), value part


kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings --group greetings-consumer-group --property print.key=true





USA - 12
IN - 16
CA - 20

SL = 11
TH = 11

goes to P0

hash code % NUM_PARTITIONS = 0

11 % 4 = 3



---------


OOP vs Functional

class Calculator {
	int sum = 0;
	
	int add(int value) {
		sum += value;
		return sum;
	}
}

calc = new Calculator();

calc.add(10); // 10
calc.add(10); // 20
calc.add(5); // 25
calc.add(5);
calc.add(10)
calc.add(5);

calc.add(5);
calc.add(10) file 20
calc.add(5);

calc.add(5);
calc.add(10)
calc.add(5); file 1400
calc.add(5);
calc.add(10)
calc.add(5); file 100
calc.add(5);
calc.add(10) file 1
calc.add(5); what is expectd output? 


unpredictable

how to predict output with milllion of lines
Big data
Cluster of machines
threading
parallism
concurrency
databases
steaming
ML
.....


// no static, no internal state
// no mutablity/no change in sum value
// pure functions
// given the same input, it should return same output
function add(int sum, int value) {
	return sum + value;
}

add(0, 10) // 10
add(0, 10) // 10

add(10, 20) // 30

add(0, 1)

add(0, 1)
add(0, 1)
add(0, 1)
add(0, 1)
add(0, 1)
add(0, 1)
add(0, 1)
add(0, 1)



---

expression vs statement


in java, if is a statement, not expression
val k = if (true) {
	  10
	}
	else {
      20
	}
	
int k = 10 + 20; // expression, return value

while -- statement, not expression

---





in scala, if, forloop, match, .... are expressions, they can return value...

