package com.example.serdes.serializer;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

@NoArgsConstructor // default constructor
public class JsonSerializer<T> implements Serializer<T> {
    // Create a Gson object to serialize the Java object into the byte stream to be stored in kafka topic by the SINK PROCESSOR
    private final Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    @Override
    public byte[] serialize(String topic, T javaObjData) {
        // convert the Java Object i.e. Player, Product, ScoreEvent into JSON string, then return as byte stream
        // to write to a Kafka output Topic
        // Note: Kafka is a byte-in and byte-out stream
        if (javaObjData == null) return null;
        return gson.toJson(javaObjData).getBytes(StandardCharsets.UTF_8);
    }
}

// Next, we need a wrapper for the JsonSerializer and JsonDeserializer
// Go to JsonSerdes class
