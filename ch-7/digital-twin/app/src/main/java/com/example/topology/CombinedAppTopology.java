package com.example.topology;

import com.example.models.DigitalTwin;
import com.example.models.Power;
import com.example.models.TurbineState;
import com.example.models.Type;
import com.example.processors.DigitalTwinProcessor;
import com.example.processors.DigitalTwinValueTransformerWithKey;
import com.example.serdes.wrapper.JsonSerdes;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

// Replacing the earlier ProcessorApp which is explicitely using ProcessorAPI with DSL (i.e KStream, KTable, map etc)
// Why: To reduce the operaitonal complexity while using some granular level access flexibility of PAPI
public class CombinedAppTopology {
    private static final Logger log = LoggerFactory.getLogger(CombinedAppTopology.class);
    public static Topology build(){ // Building Kafka Stream topology using both DSL and ProcessorAPI
        // A. StreamBuilder to construct the Kafka Topology
        StreamsBuilder topologyStreamBuilder = new StreamsBuilder();

        // Level-1
        // B. ----------------------------- Registering SOURCE Streams---------------------------------------

        // Adding 2X SOURCE PROCESSORS:: for 2X topics {reported-state-events, desired-state-events}

        // B1. Create Source Processor named "desiredStateEventSourceProcessor" for "desired-state-events"
        // Type of Source Processor: KStream // Why: Unbound stream and keyed
        // Example record: Key_1|Value_{"timestamp": "2020-11-23T09:12:00.000Z", "power": "ON", "type": "DESIRED"}

        // (a) Define the consumer role to consume the events from the desired-state-events topic
        // Notes: Need deserialization (convert records to Java objects), Need Serialization (convert Java objects to byte Stream JSON record)
        Consumed<String, TurbineState> desiredStateEventConsumptionOptions =
                Consumed.with(
                        Serdes.String(),
                        JsonSerdes.TurbineState());

        // (b) Register the events/records in a KStream, consumed by the Serdes rules defined
        // Note: We didnt set the name of this Source Processor. Why?
        // Because, Kafka Stream will automatically create an internal name for this processor for us
        // But, in PAPI definition earlier, we had to explicitely named this processor as "desired-state-events"

        //Syntax:  KStream<Key_TurbineID_String, Value_event>
        KStream<String,TurbineState> desiredStateEventSourceProcessor =
                topologyStreamBuilder.stream(
                        "desired-state-events",
                        desiredStateEventConsumptionOptions);
        getPrintStream(desiredStateEventSourceProcessor, "source_processor_desired-state-events");

        // B2. Source Processor for "reported-state-events"
        // Processor Type: KStream, Unbounded and Keyed
        // Example Record: 1|{"timestamp": "2020-11-23T09:02:01.000Z", "wind_speed_mph": 68, "power": "ON", "type": "REPORTED"}

        // (a) Define the consumer role to consume the events from the reported-state-events topic
        // Notes: Need deserialization (convert records to Java objects), Need Serialization (convert Java objects to byte Stream JSON record)
        Consumed<String, TurbineState> reportedStateEventConsumptionOptions =
                Consumed.with(
                        Serdes.String(),
                        JsonSerdes.TurbineState());

        // (b) Register the events/records in a KStream, consumed by the Serdes rules defined
        // KStream<Key_TurbineID_String, Value_event>
        // Note: We didnt set the name of this Source Processor. Why?
        // Because, Kafka Stream will automatically create an internal name for this processor for us
        // But, in PAPI definition earlier, we had to explicitly named this processor as "reported-state-events"
        KStream<String,TurbineState> reportedStateEventSourceProcessor =
                topologyStreamBuilder.stream("reported-state-events", reportedStateEventConsumptionOptions);

        getPrintStream(reportedStateEventSourceProcessor, "source_processor_reported-state-events");


        // Level-2 (see diagram): Generate shutdown signal if dangerous wind conditions are detected
        // Note: Must not genearte the SHUTDOWN signal directly to the Live Turbine, instead to a Digital Copy/ Digital Twin of the Live Turbine
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

        // Flatmap the records in the KStream
        // WHat if FlatMap is Java? -- Check my Playgroun simulation "FlatMapExample"
        /*
        Flatmap is a lazy operation, works on the Stream contents to produce outputs
        i.e. we have a List myList= [[1,2,3]], [4,5,6], [7,8,9]]        // before flatting
        After applying FlatMap, this becomes = [1,2,3,4,5,6,7,8,9]      // after flatting
         */

        // KStream<Key_1_String, Value_TurbineState_JavaObject>
        /*
        @Flatmap Operation
        Original Record from "reported-state-events":
        {"timestamp": "2020-11-23T09:02:01.000Z", "wind_speed_mph": 68, "power": "ON", "type": "REPORTED"}
        List[{"timestamp": "2020-11-23T09:02:01.000Z", "wind_speed_mph": 68, "power": "ON", "type": "REPORTED"}]

        New Record generated from Original Record 4 (shutdown signal due to high winds):
        {"timestamp": "2020-11-23T09:02:01.000Z", "wind_speed_mph": 68, "power": "OFF", "type": "DESIRED"}
        List[
        {"timestamp": "2020-11-23T09:02:01.000Z", "wind_speed_mph": 68, "power": "ON", "type": "REPORTED"},
        {"timestamp": "2020-11-23T09:02:01.000Z", "wind_speed_mph": 68, "power": "OFF", "type": "DESIRED"}
        ]

        --> After FlatMap
        List ["timestamp": "2020-11-23T09:02:01.000Z", "wind_speed_mph": 68, "power": "ON", "type": "REPORTED",
        "timestamp": "2020-11-23T09:02:01.000Z", "wind_speed_mph": 68, "power": "OFF", "type": "DESIRED"]




        @Merge the Flattterned List and the stream events in "desired-state-events" processor/topic--> Operation/intermediate result:
        {
            1|{"timestamp": "2020-11-23T09:02:01.000Z", "wind_speed_mph": 68, "power": "OFF", "type": "DESIRED"}, // Digital Twin record
            1|{"timestamp": "2020-11-23T09:12:00.000Z", "power": "ON", "type": "DESIRED"} // DesiredRecords from desired-state-events

        }
         */
        KStream<String,TurbineState> highWindFlatMapStreamProcessor =
                reportedStateEventSourceProcessor
                        .peek(
                                (key, value) ->{
                                    String turbineId = key;
                                    var windSpeedMph = value.getWindSpeedMph();
                                    var power = value.getPower();
                                    log.info("Turbinet {} has a wind speed of {} mph and a power of {}",
                                            turbineId, windSpeedMph, power);

                            }
                        )
                        .flatMapValues( // SYNTAX .flatMap((K,V)-> {return anotherValue})
                                (turbineId,originalReportedValue)->{
                                    /*
                                    Approach:
                                    (a) Original Record from "reported-state-events":
                                    {"timestamp": "2020-11-23T09:02:01.000Z", "wind_speed_mph": 68, "power": "ON", "type": "REPORTED"}
                                    --> Add this in a List
                                    List[a]

                                    (b) New Record generated from Original Record  (shutdown signal due to high winds):
                                    {"timestamp": "2020-11-23T09:02:01.000Z", "wind_speed_mph": 68, "power": "OFF", "type": "DESIRED"}
                                    --> Add this in a List
                                    List[a,b]



                                    (c) Records Read from the "desired-state-events":
                                    1|{"timestamp": "2020-11-23T09:12:00.000Z", "power": "ON", "type": "DESIRED"}
                                    --> Merge (List[a,b], c)

                                     */

                                    // Create a List as a place holder of records
                                    List<TurbineState> recordsList = new ArrayList<>();

                                    // Add the Original (unmodified) Record into the List
                                    recordsList.add(originalReportedValue);
                                    System.out.println("[Tmp(a)/] record list: "+recordsList);


                                    // Check if HighWinSpeed and if the Turbine is ON
                                    // If so, then create a Digital Twin of the Turbine  and set the Shutdown signal (Power OFF) to the digital twin
                                    // Note, there are two days to send shutdown signal to turbine
                                    // one, the processor will automatically detect it and send signal
                                    // two, you will manually send the signal from your app
                                    if (originalReportedValue.getWindSpeedMph() > 65 &&
                                            originalReportedValue.getPower()== Power.ON) {
                                        log.info("Turbine {} has detected high wind. Sending shutdown signal ...",turbineId);
                                        // Create a Digital Clone of the Turbine's reported value and set the Shutdown signal (Power OFF)
                                        TurbineState desiredTurbineRecordValue = TurbineState.clone(originalReportedValue);
                                        desiredTurbineRecordValue.setPower(Power.OFF);
                                        desiredTurbineRecordValue.setType(Type.DESIRED);

                                        // add the desiredTurbineRecordValue into the record list
                                        recordsList.add(desiredTurbineRecordValue);
                                        System.out.println("[Tmp(b)/] record list: "+recordsList);
                                    }
                                    else{
                                        System.out.println("Entering into ambuguous sectionn in Processor Topology/HighWindFlatMap ....");
                                        log.info("Turbine {} has not detected high wind. No shutdown signal will be sent...",turbineId);
                                        log.warn("[Ambiguity Resolve State] ...");
                                        TurbineState desiredTurbineRecordValue_Ambiguity = TurbineState.clone(originalReportedValue);
                                        desiredTurbineRecordValue_Ambiguity.setType(Type.DESIRED);
                                        recordsList.add(desiredTurbineRecordValue_Ambiguity);
                                    }
                                    System.out.println("[Tmp(c)/] record list: "+recordsList);
                                    return recordsList;
                                })
                        // merge originalStream/ModifiedWithDesiredValue with desiredStateEventSourceProcessor KStream to produce a larger stream
                        .merge(desiredStateEventSourceProcessor) // mostly not used, as we rarely not emitting the events in topic "desired-state-events""
                ;
        getPrintStream(highWindFlatMapStreamProcessor, "HighWindFlatMapStreamProcessor");

        // Level-3: Store the digital-twin records into a key-value persistent state store named "digital-twin-store"
        // 3.1 Create a state-store using StoreBuilder and attach it to the Topology;
        // (a) Create a StateStore
        // Unlike KTable which we store in a Materialzied state store,
        // we will use persistent key value store as our state store

        StoreBuilder<KeyValueStore<String, DigitalTwin>> storeBuilderForDigitalTwinRecords=
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("digital-twin-store"),
                        Serdes.String(), // key_1_string
                        JsonSerdes.DigitalTwin() // Value deserialzied in DigitalTwin Object to combine both "desired" and "reported" states
                        // Also serialized in Byte Stream JSON data from DigitalTwin Java Object when read from state-store
                        // Note: unlike earlier in ProcessorAPI, we dont only specify the deserailzier here
                        // infact we need both serialzier and deserialzier as these state-store data has to be written into the Dowstream Kafka SINK Processor into a new topic called "digital-twin"
                );

        // (b) Add the state-store to the topology Builder to a STREAM processor
        // Unlike PAPI, we dont need to speicify the Stream Processor name, kafka stream will create the name for us
        topologyStreamBuilder.addStateStore(
                storeBuilderForDigitalTwinRecords // State-store name "digital-twin-store"
        );
        // Next, once the state-store is defined and attached to the Topology, next is to Recreate the functionality of "DigitalTwinProcessor" as  we defined for PAPI implementations
        // Note: DigitalTwinProcesor-> creates 1:1 records for each Turbine-> combined both{DesiredState, ReportedState} under a single TurbineID i.e. 1
        // Note: it also periodically run the Punctuator to check if any of the DigitalTwin of the Turbine didnt receive a signal for over 7 days
        //        -> if so, then remove that digitalTwin from the state-store|| its a state-store cleaning step

        // 3.2 Create a DigitalTwin to combine both "desired" and "reported" states into a single "DigitalTwin" record
        // and then store it in the state-store
        // and finally send it to a downstream SINK processor to write the combined/enriched data back to the kafka stream
        // avoid using PorcessorAPI, instead use Transformer
        /*
        But, the question is, a Processor based implementation is already available in "DigitalTwinProcessor".
        Can we use that?
        Whats the problem if we use that?

        i.e if we use below
        highWindFlatMapStreamProcessor.process(
                DigitalTwinProcessor::new,
                "digital-twin-store"
        );
        ** this will not work, as we have to connect to the downstream SINK processor, which cant be done with processor based implementaiton here
        ** Processors from PAPI should only be used, when there is no link to the downstream operators.
        ** But in our case, it has to connect to the downstream SINK processor, to write the combined/enriched data back to the kafka stream
        -> Then, the solution is: to use "Transformers" | Variation to use: transformValuesWithKey

        What out combined data would look like?
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
        -> here, key -> is read only, cant be modified
        -> No need to forward multiple multiple records/events to a downstream operator, instead for 1 input -> 1 Output


         */

        // 3.2.1 Forward the stateless highWind stream (with SHUTDOWN signal in digital twin copy of Turbine) to
        // digitalTwinStreamProcessor, which combine both reported state and desried state under a a single TurbineID for each record
        // and store it the persistent key-value state-store defined above "digital-twin-store"
        // 1:1
        // Key-> read only
        // No multiple record to downstream operator
        // Using transformer
        var digitalTwinStatefulStreamProcessor =highWindFlatMapStreamProcessor
                .transformValues(DigitalTwinValueTransformerWithKey::new, "digital-twin-store");
        getPrintStream(digitalTwinStatefulStreamProcessor,"digital-twin-value-transformer-with-key");

        // Level-4: Send the combined records {desried,reported} to SINK processor to write the data basck to kafka stream topic "digital-twins"
        digitalTwinStatefulStreamProcessor.to(
                "digital-twins",
                Produced.with(
                        Serdes.String(), JsonSerdes.DigitalTwin()
                )
        );


        // Return the topology to App.main()
        Topology topology = topologyStreamBuilder.build();
        return topology;

    }

    // Generic Type K,V
    // K-> byte[]
    // V-> Tweet/EntitySentiment
    private static <K, V> void getPrintStream(KStream<K, V> kStream, String label) {
        //kStream.print(Printed.<K, V>toSysOut().withLabel(label));
        kStream.foreach((k, v) -> System.out.printf("[%s] -> %s, %s\n",label, k, v));
    }

    // method to print a kTable
    private static <K, V> void getPrintKTable(KTable<K, V> kTable, String label) {
        kTable.toStream()
                .foreach((k, v) -> System.out.printf("[%s] -> %s, %s\n",label, k, v));
    }


}


