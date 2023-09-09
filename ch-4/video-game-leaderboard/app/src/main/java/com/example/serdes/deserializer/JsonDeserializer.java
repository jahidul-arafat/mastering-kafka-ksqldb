package com.example.serdes.deserializer;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;

// This will deserialize byte-stream to json data to objects: Player, Product, ScoreEvent
// That's why Generic Type <T> is used to handle different object types through a single class
public class JsonDeserializer<T> implements Deserializer<T> {

    // Class Attributes-01
    // gson -  an instance of the Gson library used to deserialize JSON data into Java objects.
    // Deserialization - means converting byte stream into Json, Avro etc
    /* Template
        Gson gson = new Gson();
        byte[] bytes = ...; // raw bytes that we will deserialize
        Type type = ...; // ia a JAva Class used to represent deserialized record/event
        gson.fromJson(new String(bytes), type); // actually convert the raw bytes into a Java class
     */
    // Set the gson builder policy as fileName having UpperCamelCases
    // Appropriate FieldNamingPolicy is required to ensure the JSON objects are deserialized correctly
    private Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    // Class Attribute-02
    // destination class : Player, Product, ScoreEvent
    // a reference to the destination Java class that will be instantiated using the deserialized data (byte stream converted to JSON).
    private Class<T> destinationClass;

    // Class Attribute-03
    // reflectionTypeToken - This allows the deserializer to be instantiated without specifying the destination class, which can be useful in some scenarios.
    private Type reflectionTypeToken; // why this?  // check example Playground/TypeSim.java
                        // if no destination class is specified,then capture the Object type from the JSON data provided

    // constructor with only destinationClass // not lombok
    // Default Constructor needed by kafka
    public JsonDeserializer(Class<T> destinationClass) {
        this.destinationClass = destinationClass;
    }

    // Constructor that allow deserializer to work if the destination class {Player, Product, ScoreEvent} is not specified
    public JsonDeserializer(Type reflectionTypeToken){
        this.reflectionTypeToken = reflectionTypeToken;
    }

    // Overriding the Deserializer methods: configure, deserialize, close
    // method-2/Main method of the Deserializer
    // Deserialize the jsonSata we got in the byte[] array and return an instance of the destination class: Player, Product, ScoreEvent
    // Convert the rawJsonData into a Java Object
    // T- Generic - Could be either Player, Product, ScoreEvent, or any other type
    @Override
    public T deserialize(String topic, byte[] jsonData) { // Why 'topic' is here? // topic associated with the jsonData
        // there could have different deserialization logic based on the topic.
        // but here we have generalized this for all
        if (jsonData == null) return null;

        Type type = destinationClass!=null? destinationClass: reflectionTypeToken; // what if destinationClass is not specified?
        return gson.fromJson(new String(jsonData, StandardCharsets.UTF_8), type);
                                        // gson to convert rawJsonBytes into a Java Object
    }
}
