## Spark Stand alone mode

Open 3 terminals

1. Master [1]

on terminal 1

cd /opt/spark-2.4.7-bin-hadoop2.7

ls 



./sbin/start-master.sh

check 
http://localhost:8080/

Check master url 7077

3. Worker [1]

on terminal 2, only one worker

./sbin/start-slave.sh spark://ubuntu-virtual-machine:7077 


4. driver [1/spark-submit]

pyspark --master spark://ubuntu-virtual-machine:7077 


5. Spark Submit

spark-submit hello.py --master spark://ubuntu-virtual-machine:7077 



./sbin/stop-slave.sh

./sbin/stop-master.sh

