package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;


public class DslExample {
    // Define a constant
    public static final String FILENAME = "output.txt"; // transformed Stream data/event/record will be written here
    public static final String APP_TYPE = "DSL";

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

        // Since we  modified the  Deserializer for key (byte-stream to JSON), the key data should not be void and must be parsed
        KStream<String, String> kStream = builder.stream("users"); // kStream is an Unbounded stream of data/event/records


        // B. STREAM PROCESSOR
        // Objective: To transform each event/record from the Partition of the Topic "user"
        /*
         Sample input:
         > 1, jahidularafat            #<1, jahidularafat>      #<key,value>
         > 2, data1                    #<2, data1>
         > 3, data2                    #<3, data2>
         > 1, test data                #<1, 1, test data>
         */

        kStream.foreach( // replace by Lambda method referecence DslExample::StreamTransformationLogic
                (key, value) -> { // ex. value = "jahidularafat"
                    StreamTransformationLogic.commonBusinessProcessingLogic(key, value,APP_TYPE); // Transforming data, print in console and write to file
                });

        // set the required properties for running Kafka Streams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-consumer-instance-1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Key is not empty/null; deserialized (input byteStream --into-> String)
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // value is not empty/null; deserialized (input byteStream --into-> String)

        // build the topology and start streaming
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();

        // close Kafka Streams when the JVM shuts down (e.g. SIGTERM)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
