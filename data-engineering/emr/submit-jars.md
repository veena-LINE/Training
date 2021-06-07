
login to emr 

```
# do this on home directory

wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.19/postgresql-42.2.19.jar

wget https://repository.mulesoft.org/nexus/content/repositories/public/com/amazon/redshift/redshift-jdbc42/1.2.15.1025/redshift-jdbc42-1.2.15.1025.jar


mkdir jars

cd jars

wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.19/postgresql-42.2.19.jar

wget https://repository.mulesoft.org/nexus/content/repositories/public/com/amazon/redshift/redshift-jdbc42/1.2.15.1025/redshift-jdbc42-1.2.15.1025.jar

```

we have downloaded two jars into /home/hadoop/jars location.

```
cd ~ 
```

now edit the spark default config file to mention the location..

```
sudo nano  /etc/spark/conf/spark-defaults.conf
```

need to edit below properties carefully...

```
spark.driver.extraClassPath
spark.executor.extraClassPath
```

here original content/sample, 

```
......
spark.driver.extraClassPath      /usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar.............
....
spark.executor.extraClassPath    /usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/us$......
```

```
in this we need to prefix /home/hadoop/jars/* into

spark.driver.extraClassPath, spark.executor.extraClassPath , at end it would look like
```

````
spark.driver.extraClassPath /home/hadoop/jars/*:/usr/lib/hadoop-lzo/lib/*............................
spark.executor.extraClassPath /home/hadoop/jars/*:/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar............
```


spark-submit.py rds.py


