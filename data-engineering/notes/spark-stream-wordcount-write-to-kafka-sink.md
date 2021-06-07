Architecture

Spark Input Stream: Socket Server
Spark Output Sink: Kafka

Socker Server --> Spark --> Kafka


on another terminal

Kafka console consumer

```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic work-count-output

kafka-console-consumer --bootstrap-server localhost:9092 --topic work-count-output --property print.key=true --from-beginning
```

-----


```
pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0
```

```python
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
```

```
from pyspark.sql.functions import *

```

```
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count().withColumnRenamed("word", "key").withColumnRenamed("count", "value").selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
```

```
wordCounts \
    .writeStream \
    .format("kafka") \
    .outputMode("complete") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "work-count-output") \
    .option("checkpointLocation", "/home/ubuntu") \
    .start()
```


--

on another terminal 

nc -lk 9999

--

