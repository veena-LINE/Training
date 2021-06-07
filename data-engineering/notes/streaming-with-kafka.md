Ensure run pyspark or spark-submit with driver, packages details needed to integrate kafka

https://spark.apache.org/docs/2.4.0/structured-streaming-kafka-integration.html

open new terminal

```
pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0
```

-------

```
df = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", "orders")\
  .load()
 
```



```
df.writeStream.outputMode("append").format("console").start()
```

```
exit()
```

Shall print byte array, as we don't have StringDeserializer

Now go to Intellj, Run the OrderProducer.scala

---------


# With Spark deserializer with DataFrame CAST



```
pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0
```

-------

```
df = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", "orders")\
  .load()
 
```

Key is deserialized using CAST, value also deserialized using CAST operation to convert Bytes to String format.

```
dfDeserialized = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
```


```
dfDeserialized.writeStream.outputMode("append").format("console").start()
```

Shall print string key and string value

Now go to Intellj, Run the OrderProducer.scala



