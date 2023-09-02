package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DslExample {
    public static void main(String[] args) {
        // the builder is used to construct the topology
        StreamsBuilder builder = new StreamsBuilder();

        // A. SOURCE PROCESSOR
        // Objective: to read from the source topic, "users"
        // read from the source topic, "users"
        // stream/record stream--> is a DSL operator; used to view our data ; if we concerned of the entire history of messages i.e. SSH logs
        // Alternative DSL operator is --> table/changelog stream ; if we only concerned of the latest state/representation of a given key
        // table --> stateful; perform aggregations in Kafka streams; support mathematical aggregation
        // Questions: Discrepancy between the design of Kafka's Storage Layer (a distributed, append-only log) and a table?
        // Discrepancy 01: tables are updating the model data !!!!
        // Ans: Table isnt somethign we cossumed from Kafka, but something we build on the client side
        KStream<Void, String> kStream = builder.stream("users");


        // B. STREAM PROCESSOR
        // Objective: To transform each event/record from the Partition of the Topic "user"
        // for each record that appears in the source topic,
        // print the value

        // Business Logic for the transformation
        // create a function to transform the value of the record into upper case
        StringManupulator<String,String> stringManupulator = (demoString)-> demoString.toUpperCase(); // FunctionalInterface //Atomic
        Function<String,Integer> lenCalFunc = (demoString)-> demoString.length(); // Function
        Function<String, String> isTooShort = (demoString)-> {
            return lenCalFunc.apply(demoString) > 10? "Full Length" : "too Short"; //lambda function
        }; // Function


        kStream.foreach(
                (key, value) -> {
                    String formattedString = String.format("(DSL) Hello, %s. Name Length is: %d(%s) ",
                            stringManupulator.apply(value),
                            lenCalFunc.apply(value),
                            isTooShort.apply(value));
                    System.out.println(formattedString);
                });




        // you can also print using the `print` operator
        // stream.print(Printed.<String, String>toSysOut().withLabel("source"));

        // set the required properties for running Kafka Streams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Void().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // build the topology and start streaming
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();

        // close Kafka Streams when the JVM shuts down (e.g. SIGTERM)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
