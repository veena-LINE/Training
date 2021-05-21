---
title: "Spark Setup"
date: "2020-10-10"
excerpt: "Big Data Introduction"
---

## Spark Setup


Python 3.7 is needed



Download spark

```
ssh ubuntu@192.168.93.128
```


```
wget https://downloads.apache.org/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz

```

unzip and move to /opt folder

```
tar xf spark-2.4.7-bin-hadoop2.7.tgz
```

```
sudo mv spark-2.4.7-bin-hadoop2.7 /opt
sudo chmod 777 /opt/spark-2.4.7-bin-hadoop2.7
```

Set the environment Variable

```
sudo nano /etc/environment
```

Paste below
```
SPARK_HOME=/opt/spark-2.4.7-bin-hadoop2.7
```

Setup  path for user

```
cd ~
nano .profile
```

Paste below content

```
export SPARK_HOME=/opt/spark-2.4.7-bin-hadoop2.7
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```


**note** Spark directory include a Yarn, this will conflict with Hadoop yarn. To run Yarn command, use $YARN_HOME/bin/yarn instead of plain yarn command


```
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.7
```

```
python3.7

exit() to come out of python shell
```



```
cd ~

nano .profile

```
 
paste below at end of the file


```
alias python=python3.7
alias python3=python3.7
```

# For Spark to run pyspark

```
sudo ln -s /usr/bin/python3.7 /usr/bin/python
```


### Reboot Ubuntu [This is for changes in /etc/environment file, needs restart]


start pyspark

```
pyspark
```



to start spark-shell, scala

```
spark-shell
```
