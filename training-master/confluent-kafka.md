```
gitbash 

ssh into local ubuntu server
```

```
wget http://packages.confluent.io/archive/5.5/confluent-5.5.1-2.12.tar.gz
tar xf confluent-5.5.1-2.12.tar.gz
```


```
sudo mv confluent-5.5.1 /opt

sudo chmod 777 /opt/confluent-5.5.1
```

 

```
we need to change kafka environment path to the new directory
```

```
sudo nano  /etc/environment
```

REPLACE KAFKA_HOME WITH following

```
KAFKA_HOME=/opt/confluent-5.5.1
```

Save Ctrl + O and Exit Nano with Ctrl + X

```
nano  ~/.bashrc
```
REPLACE KAFKA_HOME with below

```
export KAFKA_HOME=/opt/confluent-5.5.1
```

Save Ctrl + O and Exit Nano with Ctrl + X

logout from ubuntu...

```
logout
```


```
confluent local start
```

the above command starts zookeeper, broker, kafka connectors, kafka schema registry, kafka ksql

```
http://192.168.93.128:9021/clusters
```
