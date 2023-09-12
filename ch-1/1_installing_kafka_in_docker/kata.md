# Kata Kafka Playbook
```bash
# Create a new Java Project for Kafka
gradle init \
 --type java-application \
 --dsl groovy \
 --test-framework junit-jupiter \
 --project-name my-project \
 --package com.example

# Exec into the Kafka Broker - there is only one Broker in our kafka cluster named 'kafka'
# make sure you are in the same directory residing the docker-compose file
docker-compose exec kafka bash # this is let you in the kafka Broker

# Then inside the Kafka broker, create a new named Stream or Topic called "users"
# For further parallalism, we will create a topic with "4 Partitions" with replication factor 1, means no fault tolerance and no follower configuration
# Since we are ruuning single-node kafka cluster, we set the replication factor =1
# For Production: with 3 node/broker kafka cluster, replication factor should be set to 3 for HA
kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic users \
  --partitions 4 \
  --replication-factor 1
  
# to list all the topics/named stream in the Kafka cluster
kafka-topics --bootstrap-server localhost:9092 --list

# to describe a topic/named stream in a Kafka cluster
kafka-topics --bootstrap-server localhost:9092 --describe --topic users

# output
Topic: users    PartitionCount: 4       ReplicationFactor: 1    Configs: 
        Topic: users    Partition: 0    Leader: 1       Replicas: 1     Isr: 1
        Topic: users    Partition: 1    Leader: 1       Replicas: 1     Isr: 1
        Topic: users    Partition: 2    Leader: 1       Replicas: 1     Isr: 1
        Topic: users    Partition: 3    Leader: 1       Replicas: 1     Isr: 1

# Produce some data for the given topic using Kafka-console-producer
kafka-console-producer \ 
    --bootstrap-server localhost:9092 \
    --property key.separator=, \ 
    --property parse.key=true \
    --topic users
# Data
#> 1,Ailly
#> 2, Billy
#> 3, Cilly
#> 4, Dilly
#(CTRL+C) to break

# Read the data using Kafka-console-consumer
# --from-beginning means we should start consuming from the beginning of the Kafka topic
kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic users \
    --from-beginning

#output -- showing the events information
# Jahid     -> event1
# arafat    -> event2
# billy     -> event3
# test      -> event4
# cilly     -> event5
# ailly     -> event6
# eleven    -> event7
#
##(CTRL+C) to break

# Event = {Header, Key,Timestamp, Value}
# but above, we could only see the values.
# What about to see the details
kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic users \
    --property print.headers=true \
    --property print.timestamp=true \
    --property print.key=true \
    --property print.value=true \
    --from-beginning

# Output
#CreateTime:1693454521673        1        Jahid
#CreateTime:1693454571209        1        arafat
#CreateTime:1693454578655        3        billy
#CreateTime:1693454926743        10       test
#CreateTime:1693454582027        4        cilly
#CreateTime:1693454575963        2        ailly
#CreateTime:1693454935796        11       eleven

```