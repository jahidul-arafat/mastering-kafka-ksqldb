package com.example.processors;

import com.example.models.DigitalTwin;
import com.example.models.TurbineState;
import com.example.models.Type;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

// tag: Stateful processor // for testing purpose only; Check the main implementation DigitalTwinProcessor.java class
// Objective: to combine both desired and reported states into a single record/event
// Business logics will be written here
// Input: TurbineState (having both desired and reported state); Key_1_String
// Output: DigitalTwin Object; Key_1_String
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
public class DigitalTwinProcessorWithoutPunctuator
    implements Processor<String, TurbineState, String, DigitalTwin> { // Processor is an interface; its methods need to be implemented here

    // ------------------ Define Object attributes -----------------------
    // Define the context
    private ProcessorContext<String, DigitalTwin> context;

    // Define the KeyValue store used by this STREAM statful Processor
    // Note: used state-store "digital-twin-store" which is a key-value store
    private KeyValueStore<String, DigitalTwin> kvStore;

    // ------------Override the methods()---------------------------
    // A. Initialize the Processor Context with State-store for Stateful processing
    @Override
    public void init(ProcessorContext<String, DigitalTwin> context) {
        //Processor.super.init(context);
        // a. Define the context, keep it locally
        this.context = context; // keep the context locally
                                // Note: the whole ProcessorTopology is a context and the records/events flows within the context
                                // will be used later in Punctuator and Commit
                                // Punctuator -> to remove old digital twin records that haven't updated in more than 7 days
        // b. Fetch the state-store
        // fetch the state-store attached to the "digital-twin-processor" in the ProcessTopology contect
        this.kvStore = context.getStateStore("digital-twin-store"); // state-store name "digital-twin-store"
    }

    // B. Process the record
    @Override
    public void process(Record<String, TurbineState> originalRecord) {
        // a. fetch the key and value from the record
        String key = originalRecord.key();
        TurbineState value = originalRecord.value();

        // b. create a new digital twin object
        // b1.1 Fetch the key from kvStore; if exists
        DigitalTwin digitalTwin = kvStore.get(key);
        if (digitalTwin == null) {
            // b1.2 Create a new digital twin object, if not exists
            digitalTwin = new DigitalTwin(); // NoArgument constructor
        }
        // b1.3 Check if the records in a "desired" or "reported"
        // if desired
        if (value.getType()== Type.DESIRED) {
            digitalTwin.setDesiredState(value);
        } else if (value.getType()== Type.REPORTED) {
            digitalTwin.setReportedState(value);
        }

        // c. write the new digital twin object into the state-store
        kvStore.put(key, digitalTwin); // Key_1_String

        // d. Create a new "DigitalTwin" record with this modified state
        // make sure the new record to add the original timestamp
        /*
        New Record Example:
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
        Record<String, DigitalTwin> newRecord = new Record<>(key, digitalTwin, originalRecord.timestamp());

        // forward the new record to all downstream processors within the ProcessorTopolocy context
        context.forward(newRecord);
    }

    @Override
    public void close() {
        // Processor.super.close();
        // nothing to do
    }
}