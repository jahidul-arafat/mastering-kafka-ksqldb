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
        // for each record that appears in the source topic,
        // print the value

        // iterate over each input stream, fetch the value, tranform it and save the transformed value into a file
        /*
        Make sure to exec to docker container <kafka> to produce event/record/data using the below command:
        kafka-console-producer \
          --bootstrap-server localhost:9092 \
          --property key.separator=, \
          --property parse.key=true \
          --topic users

         Sample input:
         > 1, jahidularafat            #<1, jahidularafat>      #<key,value>
         > 2, data1                    #<2, data1>
         > 3, data2                    #<3, data2>
         > 1, test data                #<1, 1, test data>
         */

        kStream.foreach( // replace by Lambda method referecence DslExample::streamTransformationLogic
                (key, value) -> { // ex. value = "jahidularafat"
                    streamTransformationLogic(key, value); // Transforming data, print in console and write to file
                });

        // you can also print using the `print` operator
        // stream.print(Printed.<String, String>toSysOut().withLabel("source"));

        // set the required properties for running Kafka Streams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev1");
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

    // Part of STREAM PROCESSOR
    // method to write a string into a text file into append only mode
    public static void appendToFile(String filename, String content) {
        String msgToConsole = String.format("Content: <%s> .... writing to file >> <%s>\n", content,filename);
        System.out.printf(msgToConsole);

        // write the content into the file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {
            writer.write(content + "\n");
        } catch (IOException e) {
            e.printStackTrace();
            // Handle the exception as needed (e.g., log it or throw it)
        }
    }

    // redundant method to write a List of Strings into a text file into append only mode
    public static void addListContentToFile(String filename, @NotNull List<String> content) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {
            for (String s : content) {
                writer.write(s + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
            // Handle the exception as needed (e.g., log it or throw it)
        }
    }

    // Part of STREAM PROCESSOR
    public static void streamTransformationLogic(String key, String content) {
        // System.out.println("Applying the Business Logic to the Stream input for Stream Transformation by the Stream Processor");
        // Business Logic for the transformation
        // create a function to transform the value of the record into upper case
        StringManupulator<String, String> stringManupulator = (demoString) -> demoString.toUpperCase(); // FunctionalInterface //Atomic
        Function<String, Integer> lenCalFunc = (demoString) -> demoString.length(); // Function
        Function<String, String> isTooShort = (demoString) -> {
            return lenCalFunc.apply(demoString) > 10 ? "Full Length" : "too Short"; //lambda function
        };

        // check if the key is NULL or not; if NULL, then replace it with a default value
        key = key == null ? "NULL" : key;    // lambda checking if key is NULL or not

        // define the predicate to check if vowel
        Predicate<Character> isVowel = c -> "aeiou".indexOf(Character.toLowerCase(c)) != -1; // Predicate to check if the character is vowel; -1 means character not found in the vowel list
        // count the vowels using this predicate
        Function<String, Long> countVowels = (demoString) -> demoString.chars()
                .mapToObj(ch -> (char) ch)// converting the IntStream <the ASCII value of a character> into Stream<Character>
                .filter(isVowel)
                .count();

        String formattedString = String.format("(DSL)<Key: %s> Value<%s>. Transformation<Name Length is: %d(%s), Vowel Count is: %d>",
                key, // key from the input stream
                stringManupulator.apply(content), // a functional interface // example value -> JAHIDULARAFAT
                lenCalFunc.apply(content),    // // 8
                isTooShort.apply(content),   // Yes
                countVowels.apply(content));  // i.e 3 vowels
        System.out.println(formattedString);

        // Writing the transformed/ enhanced stream into a file
        String formattedStringForAppendOnly = String.format("%s -> %s,%d,%s,%d",
                key, content, lenCalFunc.apply(content), isTooShort.apply(content), countVowels.apply(content));
        appendToFile(FILENAME, formattedStringForAppendOnly);
    }
}
