package com.example.topology;

import com.example.models.DigitalTwin;
import com.example.processors.DigitalTwinProcessor;
import com.example.processors.HighWindFlatmapProcessor;
import com.example.serdes.wrapper.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

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

        // ------- C. Add other processors ----------------------------
        /*
        Possible other processors could be added
        (i) REKEY Processor: a processor to perform "rekey" operations on the event stream/records
        (ii) SINK Processor: a processor to write the serialzied data back to the Kafka Stream topic
        (iii) SOURCE Processor: a processor to read from the the SINK processor topic
         */

        /*
        **** But in our cases, we would not perform the REKEY, so, no REKEY processor is required. What we will perform is:
        (i) (Stateless) STREAM Processor: a stateless high winds stream processor
            - With power=OFF (to signal Turbine shutdown) if the wind speed is greater than 65 mph, then send a shutdown signal to the downstream processors
        (ii) (Stateful) STREAM Processor: a stateful processor that saves "digital-twin" records (compised of a reported and desired state)
            - to a new key-value state-store named "digital-twin-store"; StoreBuilder is required here

            Example Data to store:
            Key_1|Value_{
              "desired": { // the HighWindFlatmapProcessor will generate this desired state
                "timestamp": "2020-11-23T09:02:01.000Z",
                "power": "OFF"
              },
              "reported": {
                "timestamp": "2020-11-23T09:00:01.000Z",
                "windSpeedMph": 68,
                "power": "ON"
              }
            }
           (iii) (Last Processor) SINK Processor, to write the enriched combined events (desired, reported) back into a Kafka topic named "digital-twin"
            as JSON Byte stream data; means only serialization is required
         * */

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

        // C2. Task. We need a way to combine this "desired" and "reported" state event into a single event/record
        // (Stateful) Add a STREAM Processor, i.e. Stateful processor that saves "digital-twin" records into a state-store
        // Note: Stateful operations in Kafka Stream requrues "state-store" for maintaining some memory of previously seen data.
        // What we are going to store in the state-store named "digital-twin-store"
        /*
        Example Data in state-store (should be):
        Key_1|Value_{
              "desired": { // the HighWindFlatmapProcessor will generate this desired state
                "timestamp": "2020-11-23T09:02:01.000Z",
                "power": "OFF"
              },
              "reported": {
                "timestamp": "2020-11-23T09:00:01.000Z",
                "windSpeedMph": 68,
                "power": "ON"
              }
            }

          Task. We need a way to combine this "desired" and "reported" state event into a single event/record
          - Why this need to be stateful?
          - bcoz records/events will arrive at different times for a given wind turbine, we have a stateful requirement
          to remembering the last recorded and desired state record for each turbine.

          Q. Whats the differnce between "state-store" creation statregies of ProcessorAPI and DSL?
          ** our earlier implementaiton in ch-4/5 was DSL based where we created state-store as below

          // State-store creation strategy in DSL
          KeyValueBytesStoreSupplier storeSupplier =
                Stores.persistentTimestampedKeyValueStore("my-store");

          grouped.aggregate(
                initializer, // initialize the core class object to store raw data in state-store;
                            //i.e. if the initializer is HighScore::new -> means data in the state-store will be the HighScore object
                adder, // aggregation logic is defined here
                        // (key, to , from) -> means i.e. data from "enrichedValue" will be merged into "highScoreValue"
                Materialized.<String, String>as(storeSupplier)); // And the merged data will be stored in state-store named "my-store"

         ** Moreover, DSL automatically creaate intermediate state-stores
         ** But processor api doesn't automatically create state-stores unless explicitely defined
         */

        // C2.1 Add the Stateful STREAM Processor "digital-twin-processor" in the Topology
        processorApiTopologyBuilder.addProcessor(
                "digital-twin-processor", // STREAM processor name
                DigitalTwinProcessor::new, // method reference // its a stream processor; thats why it needs to implement the "Processor" interface
                                                // ProcessSupplier - the actual stream function that Stream processor will execute; business logics are written there
                "high-wind-flatmap-processor", // parent-processor -01
                                                        // Check the Archtiecture Diagram;
                                                        // This Stream Processor has a hirarchy to parent processor "high-wind-flatmap-processor""
                "desired-state-events"  // parent-processor-02
                                        // Check the Architecture Diagram;

        );
        // C2.2 Use StoreBuilder to create a state-store for ProcessorAPI to store the merged "desired" and "reported" states into a single record/event
        // Create State-store for digital twin records
        StoreBuilder<KeyValueStore<String, DigitalTwin>> storeBuilderForProcessorApi=
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("digital-twin-store"),
                        Serdes.String(), // key_1_string
                        JsonSerdes.DigitalTwin() // Value deserialzied in DigitalTwin Object to combine both "desired" and "reported" states
                                                // Also serialized in Byte Stream JSON data from DigitalTwin Java Object when read from state-store
                                                // Note: unlike earlier in ProcessorAPI, we dont only specify the deserailzier here
                                                // infact we need both serialzier and deserialzier as these state-store data has to be written into the Dowstream Kafka SINK Processor into a new topic called "digital-twin"
                );

        // C2.3 Add the state-store to the STREAM processor "digital-twin-processor"
        processorApiTopologyBuilder.addStateStore(
                storeBuilderForProcessorApi, // State-store name "digital-twin-store"
                "digital-twin-processor" // STREAM processor name
                );


        // C3. (Last Processor) SINK Processor, to write the enriched combined events (desired, reported) back into a Kafka topic named "digital-twin"
        //            as JSON Byte stream data; means only serialization is required
        processorApiTopologyBuilder.addSink(
                "digital-twin-sink", // SINK processor name
                "digital-twins", // write to topic "digital-twins"
                Serdes.String().serializer(), // Key_1_String_only serailzier as we are writing back to kafka stream topic
                JsonSerdes.DigitalTwin().serializer(), // value_combined_both_desired and reported state
                "digital-twin-processor"
        );




        // Return the topology builder
        return processorApiTopologyBuilder;

    }
}
