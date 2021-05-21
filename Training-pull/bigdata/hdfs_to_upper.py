from pyspark import SparkContext

sc = SparkContext("local", "READ-UPPERCASE-WRITE")
sc.setLogLevel("WARN")


rdd_read = sc.textFile("hdfs://192.168.93.128:9000/input/README.md")
print(f"\nTotal line count = {rdd_read.count()}")

rdd_read_upper = rdd_read.map(lambda line: line.strip().upper())
print(f"\nFile content in UPPERCASE:\n{rdd_read_upper.collect()}")


def show(line):
    """Just print"""
    print(line)


# for loop - RDD style
rdd_read_upper.foreach(lambda line: show(line))

rdd_read_upper.saveAsTextFile("hdfs://192.168.93.128:9000/output/README")
