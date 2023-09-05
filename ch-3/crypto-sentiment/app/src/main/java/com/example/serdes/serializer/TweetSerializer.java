package com.example.serdes.serializer;

import com.example.model.Tweet;
import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;


// Serialization - means converting JSON data into raw bytes to store it in Kafka Stream
/*
    Gson gson = new Gson();
    gson.toJson(instance).getBytes(StandardCharsets.UTF_8); // serialize Java object into raw byte arrays
 */
public class TweetSerializer implements Serializer<Tweet> {
    // Create a Gson object to serialize the Java object into the byte stream to be stored in kafka topic by the SINK PROCESSOR
    Gson gson = new Gson();

    @Override
    public byte[] serialize(String topic, Tweet data) {
        // don't create if tweet is null
        if (data == null) return  null;

        // ele convert the Java Object i.e. Tweet into JSON string, then return as byte stream
        return gson.toJson(data).getBytes(StandardCharsets.UTF_8);

    }
}

// Next combine the serializer and deserializer into a new class called 'Serdes'
