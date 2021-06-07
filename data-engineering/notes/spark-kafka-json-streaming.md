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
  .option("subscribe", "invoices")\
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
  .option("subscribe", "invoices")\
  .load()
 
```

Key is deserialized using CAST, value also deserialized using CAST operation to convert Bytes to String format.

```
dfDeserialized = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
```
```
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

schema = StructType(
        [
                StructField("invoice_no", StringType()),
                StructField("stock_code", StringType()),
                StructField("quantity", IntegerType()),
                StructField("price", DoubleType()),
        ]
)

jsonDf = dfDeserialized.withColumn("value", from_json("value", schema))\
    .select(col('value.*'))
```

```
query = jsonDf.writeStream.outputMode("append").format("console").start()
```


if we don't want to see the spark stream output shown, we can call

```
query.stop()
```

or by using a timer..hard coded sleep

```
import time
time.sleep(30) # seconds
query.stop()
```




```
qtyGT5 = jsonDf.filter(" quantity > 5 ")

numberOfSalesDf = jsonDf.groupBy("stock_code").count()
query = numberOfSalesDf.writeStream.outputMode("complete").format("console").start()

```

Shall print string key and string value

Now go to Intellj, Run the InvoiceProducer.scala


