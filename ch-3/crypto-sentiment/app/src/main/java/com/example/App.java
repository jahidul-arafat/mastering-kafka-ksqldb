/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package com.example;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.jetbrains.annotations.NotNull;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class App {
    // Class argument to write topology description into a file
    private static final String TOPOLOGY_DESC = "src/main/resources/topology.json";

    public static void main(String[] args) {
        System.out.println("Kafka Tweet Stream Simulation");
        Topology topology = CryptoTopology.build();
        topologyDescriptor(topology);   // Writing the topology description into a file

        // set the required properties for running Kafka Streams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweeter-app-consumer-1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        //config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // build the topology with configuration properties set above
        KafkaStreams streams = new KafkaStreams(topology, config);

        // Add a shutdown hook to gracefully stop the Kafka Streams application when a global shutdown signal is received.
        //Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


        // Start the Streams
        System.out.println(" Starting Kafka Tweeter Streams");
        streams.start();
    }

    private static void topologyDescriptor(@NotNull Topology topology) {
        // describe the topology and write it to a file
        try (FileWriter fw = new FileWriter(TOPOLOGY_DESC)) {
            fw.write(topology.describe().toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
