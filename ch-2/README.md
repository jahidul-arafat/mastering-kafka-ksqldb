# Simulating the Ch-2/Hello World Kafka Stream Applicaiton
## Environment Readiness
```shell
# Step-1/Start the Kafka Cluster
# @Terminal-1

# 1.1 go to docker-compose.yaml
> docker-compose up # or
# start the Zookeeper container from the Dockerfile using the Intellij IDEA/Ultimate
# then start the Kafka container from the Dockerfile using the Intellij IDEA; depends on <ZOOKEEPER> readiness
# then start the <kafka-create-topics> container which will create a topic "users" in the <kafka> cluster

# Step-2: Let the Kafka producer produce some event/data/record which will be consumed by an instance of our 'DSLExample' application into topic "users"
# 2.1 docker exec into the kafka container; this is the main placeholder where we will execute our kafka related commands
> docker exec -it kafka bash # <kafka> is the name of the container

# 2.2 Inside the Kafka container, run the 'kafka-console-producer' command to produce some event/data/record into the kafka stream
(kafka-container)> kafka-console-producer --bootstrap-server localhost:9092 --property key.separator=, --property parse.key=true --topic users
# 2.3 Now enter the below inputs into the kafka-console-producer prompt
  > 1, Sample Data        # <key,value> pair, key must not be empty
  > 2, Laplus
  > 3, this is a kafka simulation
  > 4, by Jahidul Arafat, OK

```
## Start the DSLExample application
```shell
# @ Terminal-2

# 3. Fist build the applicaiton using grade build
# 3.1 Check the 'build.gradle' file under the project directory to check if all the dependencies are satisfied
# 3.2 build the application
> gradle build

# 3.3 Start the application
> ./gradlew runDSL --info
```

## Tear-down the Kafka cluster
```shell
> docker-compose down # this will destroy the container ZOOKEEPER, Kafka
```