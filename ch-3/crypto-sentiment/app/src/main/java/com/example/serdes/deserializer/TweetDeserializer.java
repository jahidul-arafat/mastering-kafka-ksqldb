package com.example.serdes.deserializer;

import com.example.model.Tweet;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;


// Tweet received through Kafka Connector
/*
{
    "CreatedAt":1577933872630,                                                  // picked for deserialization
    "Id":10005,                                                                 // picked for deserialization
    "Text":"Bitcoin has a lot of promise. I'm not too sure about #ethereum",    // picked for deserialization
    "Lang":"en",                                                                // picked for deserialization
    "Retweet":false,                                                            // picked for deserialization
    "Source":"",
    "User":{
        "Id":"14377870",
        "Name":"MagicalPipelines",
        "Description":"Learn something magical today.",
        "ScreenName":"MagicalPipelines",
        "URL":"http://www.magicalpipelines.com",
        "FollowersCount":"248247",
        "FriendsCount":"16417"}
        }
}


 */
// Deserialization - means converting byte stream into Json, Avro etc
/*
    Gson gson = new Gson();
    byte[] bytes = ...; // raw bytes that we will deserialize
    Type type = ...; // ia a JAva Class used to represent deserialized record/event
    gson.fromJson(new String(bytes), type); // actually convert the raw bytes into a Java class
 */
public class TweetDeserializer implements Deserializer<Tweet> {
    // Set the gson builder policy as fileName having UpperCamelCases
    // Appropriate FieldNamingPolicy is required to ensure the JSON objects are deserialized correctly
    private Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
            .create();


    // deserializer returns an instance of Tweet class from the JSON data
    @Override
    public Tweet deserialize(String topic, byte[] data) {
        if (data == null) return null; // dont deserialize if bytes are null
        return gson.fromJson(new String(data, StandardCharsets.UTF_8), Tweet.class); // using Gson library to deserialize the byte array into a Tweet object
    }
}
