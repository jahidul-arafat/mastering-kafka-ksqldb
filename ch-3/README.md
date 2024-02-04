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

# Schema Registry: https://docs.confluent.io/platform/current/schema-registry/index.html
# Schema Registry provides a centralized repository for managing and validating schemas for topic message data, 
# and for serialization and deserialization of the data over the network. 
# Producers and consumers to Kafka topics can use schemas to ensure data consistency and compatibility as 
# schemas evolve. Schema Registry is a key component for data governance, helping to ensure data quality, 
# adherence to standards, visibility into data lineage, audit capabilities, 
# collaboration across teams, efficient application development protocols, and system performance.

# A AVRO Schema Registry is live at port 8081 for serlaiziation and deserialization
# to see its contents
# the avro schema name "crypto-sentiment-value" is created automatically based on the topic name "crypto-sentiment" where the SINK processor is writing the contens
# contents are serialized to byte[] when writing back to the kafka topic.
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/crypto-sentiment-value/versions/latest
---
# output:
// 20240204154408
// http://localhost:8081/subjects/crypto-sentiment-value/versions/latest

{
  "subject": "crypto-sentiment-value",
  "version": 1,
  "id": 1,
  "schema": "{\"type\":\"record\",\"name\":\"EntitySentiment\",\"namespace\":\"com.example.model\",\"fields\":[{\"name\":\"created_at\",\"type\":\"long\"},{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"entity\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"text\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"sentiment_score\",\"type\":\"double\"},{\"name\":\"sentiment_magnitude\",\"type\":\"double\"},{\"name\":\"salience\",\"type\":\"double\"}]}"
}

curl http://localhost:8081/subjects/crypto-sentiment-value/versions/1
curl http://localhost:8081/schemas/ids/1
curl http://localhost:8081/config/crypto-sentiment-value # doesnt work for my self deployed schema registry at localhost:8081

```