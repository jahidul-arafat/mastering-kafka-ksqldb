package com.example.serdes.serializer;

import com.example.model.EntitySentiment;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serde;

import java.util.Collections;
import java.util.Map;

/*
Goal: To serialized the sentimentEnrichedRecord generated from the Avro "EntitySentiment" data-class to byte-stream
to write into the target Kafka Topic "Crypto-Sentiment"

Objective: Schema Registry-aware Avro Serdes requires some additional configuration
- Improve code readability by creating a factory class for instantiating Serdes instance
   for each of the data class i.e. "EntitySentiment" in the project
 */

// Create a registry-ware Avro Serde for the EntitySentiment Avro Class
// This registry will be deployed in http://localhost:8081
public class AvroSerde {
    // Target: AvroSerde.EntitySentiment("http://localhost:8081", false)
    public static Serde<EntitySentiment> EntitySentiment(String url, boolean isKey) {
        // get the Avro registry configurations into a Map
        Map<String, String> avroSerdeConfig = Collections.singletonMap("schema.registry.url", url);

        // Instantiate Avro Serde (Along with the avroSerdeConfiguration) for the EntitySentiment Avro Data Class
        Serde<EntitySentiment> entitySentimentSerde = new SpecificAvroSerde<>();
        entitySentimentSerde.configure(avroSerdeConfig, isKey);

        // return the EntitySentiment Serde instance
        return entitySentimentSerde;
    }
}
