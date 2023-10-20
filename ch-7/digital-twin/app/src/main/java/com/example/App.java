package com.example;

import com.example.rebalancing.CustomStateRestoreListener;
import com.example.restful_services.DigitalTwinRestService;
import com.example.serdes.wrapper.JsonSerdes;
import com.example.topology.ProcessorAppTopology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;

public class App {
    //private static final Config config = ConfigFactory.load();
    private static final Logger log = LoggerFactory.getLogger(App.class);
    public static void main(String[] args) {
        // Call the Topology Build to build the kafka Stream Topology
        Topology topology = ProcessorAppTopology.build();

        // For RESTful API calls
        // Override the system properties
        // we allow the following system properties to be overridden
        String host = System.getProperty("host");
        Integer port = Integer.parseInt(System.getProperty("port"));
        String stateDir = System.getProperty("stateDir");
        String endpoint = String.format("%s:%s", host, port);

        // set the required properties for running Kafka Streams
        Properties props = new Properties();
        //config.entrySet().forEach(e -> props.setProperty(e.getKey(), config.getString(e.getKey())));
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev-consumer");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // not reading the earliest events in a topic-partition // only read the latest
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, endpoint);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        // --------- BUILDING TOPOLOGY and START STREAMING------------------------
        System.out.println("Starting Digital Twin Streams App");
        KafkaStreams streams = new KafkaStreams(topology, props);

        // A. Define the Rebalacing strategies
        // How to improve the visibility of Kafka Stream Applicaiton
        // how to listen to rebalance triggers in our Kafka Streams application

        // A1.1 Approach-01/ State Listener: To monitor Kafka Stream applications state
        // i.e. when a rebanacing is tiggered which is impctful for Stateful applicaitons
        // Example: @MailChimp, they has a speical matrix that gets incremented when a Rebalancing is triggered and they connect that to Promethues
        // Kafka Stream Applicaiton States:
        // (a) Created -> Not Running
        // (b)Created -> Running -> Error-> Pending Shutdown-> Not Running
        // (c)Created -> Running -> Reblancing -> Pending Shutdown -> Not Running
        streams.setStateListener(
                (oldState, newState) ->{
                    if (newState.equals(KafkaStreams.State.REBALANCING)){
                        log.info("Rebalancing due to application state changes");
                    }
                }
        );

        // A1.2 Approach-02/ State restore listener example
        streams.setGlobalStateRestoreListener(new CustomStateRestoreListener());

        // B. Cleanup the local state
        // clean up local state since many of the tutorials write to the same location
        // you should run this sparingly in production since it will force the state
        // store to be rebuilt on start up
        streams.cleanUp();

        log.info("Starting Digital Twin Streams App");
        streams.start();
        // close Kafka Streams when the JVM shuts down (e.g. SIGTERM)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // REST Service- Interactive Query
        @SuppressWarnings("StringSplitter")
        //String[] endpointParts = config.getString(StreamsConfig.APPLICATION_SERVER_CONFIG).split(":");
        //HostInfo hostInfo = new HostInfo(endpointParts[0], Integer.parseInt(endpointParts[1]));
        HostInfo hostInfo = new HostInfo(host, port);
        DigitalTwinRestService service = new DigitalTwinRestService(hostInfo, streams);
        log.info("Starting Digital Twin REST Service");
        service.start();

    }


}
