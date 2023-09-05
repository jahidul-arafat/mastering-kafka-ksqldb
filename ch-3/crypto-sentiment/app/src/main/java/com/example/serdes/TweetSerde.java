package com.example.serdes;

import com.example.model.Tweet;
import com.example.serdes.deserializer.TweetDeserializer;
import com.example.serdes.serializer.TweetSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

// Purpose: To combine both tweeter serializer and deserializer into a custom serde class
// This serde class will act as a wrapper class for Kafka streams to use
// WRAPPER CLASS
public class TweetSerde implements Serde<Tweet> {
    @Override
    public Serializer<Tweet> serializer() {
        return new TweetSerializer();
    }

    @Override
    public Deserializer<Tweet> deserializer() {
        return new TweetDeserializer();
    }
}
