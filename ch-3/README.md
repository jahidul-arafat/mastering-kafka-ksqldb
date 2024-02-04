# mastering-kafka-ksqldb
OReilly Implementation by Jahidul Arafat as a part of High Performance Computing.

Visualize Kafka Stream Topology: https://zz85.github.io/kafka-streams-viz/
```bash
# To setup: Make sure to run the docker-compose.yml file
# comment the kafka-create-topics and schema-registry script in the docker-compose.yml, then run
docker-compose up 
# then uncomment the sections and create the containers for kafka-create-topics, which create the necessary kafka topics
# and schema-registry, when will help to serialize the enrichedSentimentRecirds into the target Kafka topic using Confluent registry
# This schema-registry will be running at localhost:8081

# at Kafka Container which is running at localhost:9092 or kafka:9092
# Produce some records into the Kafka Topic "Tweets"
kafka-console-producer --bootstrap-server localhost:9092 --topic tweets < test.json

# at Kafka Avro registry container which is running at localhost:8081
# Consume the sentiment enriched records from the Kafka Topic "crypto-sentiment"
kafka-avro-console-consumer --bootstrap-server kafka:9092 --topic crypto-sentiment --from-beginning

# to stop the docker setup
docker-compose down

```