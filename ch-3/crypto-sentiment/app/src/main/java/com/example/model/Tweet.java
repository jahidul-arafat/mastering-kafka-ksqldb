package com.example.model;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
// Task-1: Define a Data class/POJO named 'Tweet' for deserialization
/*
    The fields that we want to deserialize are:
    - createdAt -- from raw byte field 'CreatedAt'
    - id -- from raw byte field 'Id'
    - lang -- from raw byte field 'Lang'
    - retweet -- from raw byte field 'Retweet'
    - text -- from raw byte field 'Text'
 */


// Tweet - the java class developed to represent deserialized object
// serializer and deserializer classes are often combined into a single class called a /Serdes/
// Deserialization - means converting byte stream into Json, Avro etc
/*
    Gson gson = new Gson();
    byte[] bytes = ...; // raw bytes that we will deserialize
    Type type = ...; // ia a JAva Class used to represent deserialized record/event
    gson.fromJson(new String(bytes), type); // actually convert the raw bytes into a Java class
 */
// Serialization - means converting JSON data into raw bytes to store it in Kafka Stream
/*
    Gson gson = new Gson();
    gson.toJson(instance).getBytes(StandardCharsets.UTF_8); // serialize Java object into raw byte arrays
 */
@Data
@AllArgsConstructor
@NoArgsConstructor // default constructor
public class Tweet {
    @SerializedName("CreatedAt") // Gson's SerializedName annotation; This allows Gson to deserialize JSON objects into Java Objects with different field name
    private Long createdAt;

    @SerializedName("Id")
    private Long id;

    @SerializedName("Lang")
    private String lang;

    @SerializedName("Retweet")
    private Boolean retweet;

    @SerializedName("Text")
    private String text;
}
