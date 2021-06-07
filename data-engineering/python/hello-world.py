from pyspark import SparkContext

sc = SparkContext("spark://ubuntu-virtual-machine:7077", "HelloApp")
rdd = sc.parallelize(['hello', 'world'])

print (rdd.collect())
