Day 5




numbers [parent rdd]
	odd [child rdd, parent rdd]
		odd * 11 [child]
			collect....
			

before


num partititons = 4 

p0: [1, 2, 3]
p1: [4, 5, 6]
p2: [7, 8, 9]
p3: [10]


after PartitionBy(2) - move the data into partitions

p0: [1, 2, 3, 4, 5 , 6]
p1: [7, 8, 9, 10]

-----


1 million orders data, all over world order
	USA order 50000
	rest of the world 950000

not given any specific parition

8 paritions

Spark will pass over all 8 partitions data to filter
scan over all 1 million records, 
filter (lambda country == 'USA')
pick and process 50 K

----

parition

Move all usa data into partition 0

P0: [USA1, USA2.....USA50000, CA]
P1: [IN1,...10000]
...

scan over P0, 60 K records of USA + CA, 50K USA
filter (lambda country == 'USA')
pick and process 50 K

groupBy, 200 paritions




----

local/master[2] - 2 threads

two task at a time

2 partitions is default

1 KB - 2 partitions
1 GB - 2 partitions
1 TB  - 2 partitions

2 parititons data is moved itno 100 partitions
rdd.partitionBy(100)

df.repartitions(columnName, 20)

rdd.groupBy - default 200 paritions



ds1 = [  [  'mobile', 10], [  'mobile', 5], [  'tablet', 4], [ 'tablet', 3]       ]

groupBy

mobile: [
			10
			5
]

tablet: [4, 3]

keys " mobile, tablets"
values:  
		[10,5] - iterable/collection, sum ( 10 + 5)
		[4,3] - iterable
		
		
Python 
	yield
	itr
	
collection: 1 millions [1, 2,...1000000000] - memory, time to generate

itr: produce / generate when it called
		itr.next()/enum.key()


------

Python

100 million numbers
	loaded from file
	loaded from stream
	generated on need basics
	
l = [range(100000000000)]
	   create memory, garbage collector
	
I am not using in code

l[:10] - 10 numbers

---

yield, generator, iterator



generator func
	yield 
		produce value on demand
		
iter = generator()

next(iter)

for (i in iter)
		







Unstructured data
    logs, notes..emails
Semi-structred data
    csv....

    Apply RDD first, clean, filter, massage data
        and convert to DataFrame

            DataFrame - Structured Data 
                            Row
                            Column
                            Name
                            Data Type
                            schema...

RDD - Low Level

Spark - Object Encoder - Serialization [friendly with python/java/jvm]
         DataFrame using encoded objects which RDD
		 
		 
		 
---

load all use zip codes csv file
skip the first record

read the data

Get number of cities


identify unique states 
identify number of states
identify number of cities across country
List out all the cities across country
identify number of cities in each state



1 TB 

code, city, country, state, lat, lang
.....
.....

-- load 1 TB and process 1 TB

my use , i need only state, city

map lambda r: (r[1], r[2]) - outout 6 columns, we pick only 2

1 TB reduced into 200 GB


# embeded mode

pyspark/spark-shell 
		All 3 components run in single JVM Process
					- Driver 
					- Master
					- Worker
					
## Spark Stand alone mode

Open 3 terminals

1. Master [1]

on terminal 1

./sbin/start-master.sh


3. Worker [1]

on terminal 2, only one worker

./sbin/start-slave.sh spark://ubuntu-virtual-machine:7077 


4. driver [1/spark-submit]

pyspark --master spark://ubuntu-virtual-machine:7077 
