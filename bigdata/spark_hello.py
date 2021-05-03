from pyspark import SparkContext


# Create SparkContext
# local -> standalone, spark driver and executor in the same JVM process
sc = SparkContext("local", "sc_hello")  # --> indicates Spark to iuse just 1 core (by default)
# sc = SparkContext("local[2]", "sc_hello")  # --> indicates Spark to use 2 cores
# sc = SparkContext("local[*]", "sc_hello")  # --> indicates Spark to use all CPU cores

sc.setLogLevel("WARN")  # Log only WARNing and ERRORs

data = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]

data_rdd = sc.parallelize(data)
print("-----------------------------------------------------------")
print("Max", data_rdd.max())


print("****INFO**** PARTITIONS = ", data_rdd.getNumPartitions())
print("-----------------------------------------------------------")



# How to run?
# $spark-submit spark_hellp.py
