

wget http://packages.confluent.io/archive/5.5/confluent-5.5.1-2.12.tar.gz


tar xf confluent-5.5.1-2.12.tar.gz


sudo mv confluent-5.5.1 /opt

sudo chmod 777 /opt/confluent-5.5.1


sudo nano /etc/environment

paste below

```
KAFKA_HOME=/opt/confluent-5.5.1
CONFLUENT_HOME=/opt/confluent-5.5.1
```

----

cd ~ 

nano .bashrc 



paste below at end of the file

```
export KAFKA_HOME=/opt/confluent-5.5.1
export CONFLUENT_HOME=/opt/confluent-5.5.1

export PATH=$PATH:$KAFKA_HOME/bin
```
