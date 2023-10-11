package com.example.topology;

import com.example.processors.HighWindFlatmapProcessor;
import com.example.serdes.wrapper.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;

// this topology will be called by the "ProcessorApp"
public class ProcessorAppTopology {
    // define the non DSL i.e. Processor API topology
    /*
    - in DSL specific topology, we had to create a separate Topology class called "PatientMonitoringTopology"
        - and a build() method inside it which used StreamBuilder to build the kafka topology,
        - where we can register the SOURCE processors in the topology either using KStream or KTable based on whether the event stream is unbounded and keyed or not
     - But in this case, we just cant simply do it

     */
    public static Topology build() {
        // A. Instantiate a topology instance directly instead of using StreamBuilder as we used in DSL
        // Note:    - DSL requires to instantiate a StreamBuilder object, we we would ommit here
        //          - Because DSL has DSL specific langauge for map, flatmap, merge, branch etc which is available with StreamBuilder
        //          - Thats why in DSL we had to use StreamBuilder

        // But in ProcessorAPI topology, we need to create a Topology instance directly instead of using StreamBuilder as we used in ProcessorAPI
        // We would not use any Domain Specific Langauge i.e. map, flatmap, merge etc
        Topology processorApiTopologyBuilder = new Topology();

        // ------------B. Registering SOURCE PROCESSORS------------------------
        // 2x source processors: (i) SOURCE: reported-state-event, (ii) SOURCE: desired-state-event
        /* Note that, there is no specific mention of KStream or KTabkle when adding SOURCE processors in the TopologyBuilder
        - Why?
        - This KStream/KTable abstraction was avaible in DSL based solution as we could see in the PatientMonitoringApp
        - But in ProcessorAPI based solution, we cant use these KStream/KTable abstractions
        - Unlike DSL Based solution, the processors in ProcessorApiTopology are not 'Stateful' and not connected to a state-store
            - Being stateless, they cant re,eber the latest state of a given key of an event-stream/record
        * */

        // B1. Explicitly Add a source processor for topic "desired-state-event" | Note. in DSL as in "PatientMonitoringApp" this explicit definition is not required
        // The source processor will read the event/records from this topic and downstream to the main application
        /*
        Example records:
        1|{"timestamp": "2020-11-23T09:12:00.000Z", "power": "OFF", "type": "DESIRED"}
         */
        processorApiTopologyBuilder.addSource(
                "desired-state-events", // name of the source processor // should be unique in application lifecycle
                                                // Why this "source processor name" has to be unique?
                                                // because, under the hood, Kafka Streams store these names in a topologically sorted map
                                                // Note: there could be a ParentProcessor-ChildProcessor mapping, which we will see later; so naming is important
                Serdes.String().deserializer(),// key deserializer // Here, key=1, which need to be deserialized as String
                JsonSerdes.TurbineState().deserializer(), // value deserializer // here, value = {"timestamp": "2020-11-23T09:12:00.000Z", "power": "OFF", "type": "DESIRED"}
                                                // *** See, here we only used the Deserializer to deserialize JSON Byte stream data into TurbineState object
                                                // ** But in the DSL specificiation, we had to pass the whole JsonSerdes wrapper contains both the Deserializer and Serializer
                                                // So, ProcessorAPI gives us the fine-grained control over the DSL Specification
                // value needs to be deserialized as TurbineState object
                "desired-state-events" // name of the topic to read from
        );

        // B2. Explicitly Add a source processor for topic "reported-state-event" | Not required if DSL
        // The source processor will read the event/records from this topic and downstream to the main application
        /*
        Example records:
        1|{"timestamp": "2020-11-23T09:02:00.000Z", "wind_speed_mph": 40, "power": "ON", "type": "REPORTED"}
         */
        processorApiTopologyBuilder.addSource(
                "reported-state-events", // name of the source processor: check its child processor "high-wind-flatmap-processor"
                Serdes.String().deserializer(),// key deserializer // Here, key=1, which need to be deserialized as String
                JsonSerdes.TurbineState().deserializer(), // value deserializer // here, value = {"timestamp": "2020-11-23T09:02:00.000Z", "wind_speed_mph": 40, "power": "ON", "type": "REPORTED"}
                // value needs to be deserialized as TurbineState object
                "reported-state-events" // name of the topic to read from
        );

        // ------- C. Add other procesors ----------------------------
        /*
        Possible other processors could be added
        (i) REKEY Processor: a processor to perform "rekey" operations on the event stream/records
        (ii) SINK Processor: a processor to write the serialzied data back to the Kafka Stream topic
        (iii) SOURCE Processor: a processor to read from the the SINK processor topic
         */

        /*
        **** But in our cases, we would not perform the REKEY, so, no REKEY processor is required. What we will perform is:
        (i) STREAM Processor: a stateless high winds stream processor
            - With power=OFF (to signal Turbine shutdown) if the wind speed is greater than 65 mph, then send a shutdown signal to the downstream processors
        (ii) STREAM Processor: a stateful processor that saves "digital-twin" records (compised of a reported and desired state)
            - to a new key-value state-store named "digital-twin-store"; StoreBuilder is required here

            Example Data to store:
            Key_1|Value_{
              "desired": {
                "timestamp": "2020-11-23T09:02:01.000Z",
                "power": "OFF"
              },
              "reported": {
                "timestamp": "2020-11-23T09:00:01.000Z",
                "windSpeedMph": 68,
                "power": "ON"
              }
            }
         */

        // C1. (Stateless) Add a STREAM Processor, i.e. Stateless high wind stream processor
        // Purpose: For generating SHUTDOWN signals when our wind turbine is reporting dangerous wind speeds. i.e.
        // if WindSpeedMph >65, then SHUTDOWN signal to turbine
        /*
        addProcessor(final String name,
                     final ProcessorSupplier<KIn, VIn, KOut, VOut> supplier, // this is a functional interface // only one abstract method + multiple default/static methods
                     final String... parentNames) {
         */
        processorApiTopologyBuilder.addProcessor(
                "high-wind-flatmap-processor", // STREAM processor name
                HighWindFlatmapProcessor::new, // method reference // its a stream processor; thats why it needs to implement the "Processor" interface
                                                // ProcessSupplier - the actual stream function that Stream processor will execute; business logics are written there
                "reported-state-events" // Check the Archtiecture Diagram;
                                                    // This Stream Processor has a hirarchy to parent processor "reported-state-events"
        );


        // Return the topology builder
        return processorApiTopologyBuilder;

    }
}
