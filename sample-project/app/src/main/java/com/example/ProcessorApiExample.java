package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

// ProcessorApi lacks of the abstractions provided by DSL
public class ProcessorApiExample {
    public static void main(String[] args) {
        // Create a Processor Topology
        Topology topology = new Topology(); // Unlike DSLExample, we dont need any StreamBuilder here

        // A. SOURCE PROCESSOR
        // add a source processor into the topology which reads from a Kafka topic "users"
        topology.addSource("UserSource", "users");

        // B. STREAM PROCESSOR
        // add a Stream Processor into the topology which transforms the input data read by source processor from topic "users"
        topology.addProcessor("SayHello", SayHelloProcessor::new, "UserSource");


        // set the required properties for running Kafka Streams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-consumer-instance-2");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // build the topology and start streaming
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();



    }
}
