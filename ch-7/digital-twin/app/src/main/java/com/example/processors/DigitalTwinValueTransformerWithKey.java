package com.example.processors;

import com.example.models.DigitalTwin;
import com.example.models.TurbineState;
import com.example.models.Type;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

// tag: Stateful processor with Punctuator to minimize and clean the "digital-twin-store" state store periodically
// Objective: to combine both desired and reported states into a single record/event
// Business logics will be written here
// Input: TurbineState (having both desired and reported state); Key_1_String
// Output: DigitalTwin Object; Key_1_String
// Ensure Thread Safety: The "Processor" and "Punctuator" will be under same thread; so there will be no two thread, thus no concurrency issues

/*
Expected New Record Generated by this DigitalTwinProcessor
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
 */

public class DigitalTwinValueTransformerWithKey
    implements ValueTransformerWithKey<String, TurbineState, DigitalTwin> {
    // Define the logger -- Class Attribute
    private static final Logger log = LoggerFactory.getLogger(DigitalTwinProcessor.class);

    // ------------------ Define Object attributes -----------------------
    // Define the context
    private ProcessorContext context; // changes: in PAPI it was private ProcessorContext<String,DigitalTwin> context;

    // Define the KeyValue store used by this STREAM stateful Processor
    // Note: used state-store "digital-twin-store" which is a key-value store
    private KeyValueStore<String, DigitalTwin> kvStore; // Key_1, Value_both_reported_and_desired_state_of_turbine

    // Define the Punctuator
    // Purpose: To remove all digital-twin records from the state-store that haven't updated in more than 7 days
    // You should have control on the Punctuator to stop it at any time or later; thats why return type is "Cancellable"
    private Cancellable punctuator;
    @Override
    @SuppressWarnings("unchecked") // means to avoid warning in this method
    public void init(ProcessorContext context) {
        log.info("Initializing both ProcessorContext and Punctuator(Cleaner) in the same thread; So concurrecny issue");
        //Processor.super.init(context);

        // a. Define the context, keep it locally
        this.context = context; // keep the context locally
        // Note: the whole ProcessorTopology is a context and the records/events flows within the context
        // will be used later in Punctuator and Commit
        // Punctuator -> to remove old digital twin records that haven't updated in more than 7 days



        // For Punction purpose, not for merging the digital-twin/desired_state_event with reposted-state-event into "digital-twin-store"
        // b. Fetch the state-store
        // fetch the state-store attached to the "digital-twin-processor" in the ProcessTopology context to prune the old data
        this.kvStore = context.getStateStore("digital-twin-store"); // state-store name "digital-twin-store"

        // c. Initialize the Punctuator within the ProcessorTopology context
        // Schedule a punctuate() method every 5 mins based on local system wall clock time to clean up the state-store digital twin records not seen an update over 7 days
        log.info("Initializing Punctuator; Punctuator will run every after 5 mins to clean the state-store");
        this.punctuator = this.context.schedule(
                Duration.ofMinutes(5),
                PunctuationType.WALL_CLOCK_TIME,
                this::enforceTtl // this is an Object method; you have to create this enforceTtl() method below
                // this will iterate over all records in the state-store
                // then fetch the last reported state of each record to check if any update recrived within 7 days for that record
                // if not (no update received for that state-store over that last 7 days), then delete that record
        ); //


        // d. Commit
        // commit progress every 20 seconds. commiting progress means committing the offset for the task
        //
        // note that the commit may not happen immediately,
        // it's just a request to Kafka Streams to perform the commit when it can. for example,
        // if we have downstream operators from this processor, committing immediately from here
        // would be committing a partially processed record. therefore, Kafka Streams will determine
        // when handle the commit request accordingly (e.g. after a batch of records have been
        // processed)
        //
        // note that these commits happen in addition to the commits that Kafka Streams performs
        // automatically, at the interval specified by commit.interval.ms
        this.context.schedule(
                Duration.ofSeconds(20), PunctuationType.WALL_CLOCK_TIME, (ts) -> context.commit());

    }

    // Key-> is read only -> immutable
    // Record -> is mutable
    // Stategy
    /*
    - Check of the recordValue is NULL or not;
        - if NULL, just return
    - Using the key, fetch the digitalTwin record from the state-store;
        - if no DigitalTwin record found associated with the TurbineID, then create a DigitalTwin object
        - Note: DigitalTwin object has two attributes: reportedState and desiredState
        - Check the Type of OriginalValue :
            - if it is in DesiredState, set the desiredObject into the DigitalTwin object
            - if it is in ReportedState, set the reportedObject into the DigitalTwin object
     - Finally store the DigitalTwin object into the key-value store and return the DigitalTwin object

     */
    @Override
    public DigitalTwin transform(String readOnlyRecordKey, TurbineState originalRecordValue) {
        // a. Logging the Original record Value
        System.out.println("[@DigitalTwinProcessor:: Key: "+ readOnlyRecordKey+ "/ Processing:] " + originalRecordValue);


        // b. Check if the value TYPE is a "desired" or "reported" state or null
        // if null, just return from the method; because the TYPE is not set
        if (originalRecordValue == null || originalRecordValue.getType() == null) {
            log.warn("Skipping state update due to unset value type (must be: desired, reported)");
            return null;
        }

        // c. create a new digital twin object
        // c1.1 Fetch the key from kvStore; if exists
        DigitalTwin digitalTwin = kvStore.get(readOnlyRecordKey);
        if (digitalTwin == null) {
            // c1.2 Create a new digital twin object, if not exists
            digitalTwin = new DigitalTwin(); // NoArgument constructor
            System.out.println("[@DigitalTwinProcessor]: DigitalTwin object(CREATING): "+digitalTwin);
        }
        // c1.3 Check if the records in a "desired" or "reported"
        // if desired
        if (originalRecordValue.getType() == Type.DESIRED) {
            digitalTwin.setDesiredState(originalRecordValue); //passing the whole value object "TurbineState"
            System.out.println("[@DigitalTwinProcessor]: DigitalTwin object(DESIRED): "+digitalTwin);
        } else if (originalRecordValue.getType() == Type.REPORTED) {
            digitalTwin.setReportedState(originalRecordValue); // passing the whole value object "TurbineState"
            System.out.println("[@DigitalTwinProcessor]: DigitalTwin object(REPORTED): "+digitalTwin);
        }

        // d. write the new digital twin object into the state-store
        log.info("[Storing digital twin record into state-store]: "+ digitalTwin);
        kvStore.put(readOnlyRecordKey, digitalTwin); // Key_1_String


        // forward the new record to all downstream processors within the ProcessorTopolocy context
        log.info(">> Forwarding new record to downstream processors within the ProcessorTopology context");
        return digitalTwin;
    }

    @Override
    public void close() {
        log.warn("Cancelling the Punctuator because ProcessorTopology is closing....");
        punctuator.cancel(); // cancel the punctuator when our processor is closed

    }

    // --------- Custom Methods ----------------------------------
    // 1. As a delete marker like tombstone in DSL implementation
    // to delete the old records from the state-store to keep it minimized
    public void enforceTtl(long timestamp) {
        // a. fetch all "digital-twin-store" state-store records
        // To avoid resource leakage, wrap it with try-with-resources clause
        /*
        What the record looks like in the state-store: an example
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
         */
        // try-with-resource statement
        try (KeyValueIterator<String, DigitalTwin> recordIterator = this.kvStore.all()) {
            // b. iterate over each record
            while (recordIterator.hasNext()) {
                // b1.1 Get the current record
                KeyValue<String, DigitalTwin> currentRecord = recordIterator.next();
                log.info("Checking to see if record: "+ currentRecord.key+ " will expire or not");

                // b1.2 Get the last reported state of the current record; means the last reported timestamp, windspeed, power
                TurbineState lastReportedStateOfCurrentRecord = currentRecord.value.getReportedState();

                // b1.3 Check if the last reported state of the current record is NULL or not
                // i. if NULL, just continue to the next record
                if (lastReportedStateOfCurrentRecord == null) {
                    continue;
                }
                // ii. If Not NULL, then check if the last reported state of the current record is older than 24 hours and delete that record
                // get the last update time stamp of the current record
                Instant lastUpdateTimeStamp = Instant.parse(lastReportedStateOfCurrentRecord.getTimestamp());
                // check if its more than 24 hours
                long hoursSinceLastUpdate = Duration.between(lastUpdateTimeStamp, Instant.now()).toHours();
                log.info("hours Since Last Update: " + hoursSinceLastUpdate + " hours");
                if (hoursSinceLastUpdate >= 24) {
                    // delete the current record
                    log.info("Deleting record: "+ currentRecord.key+ " / No signal in Digital Twin for 24 hours");
                    this.kvStore.delete(currentRecord.key); // just delete the Key from this key-value state-store
                }
            } // while iterator ends
        } // try-with-resource ends
    }
}