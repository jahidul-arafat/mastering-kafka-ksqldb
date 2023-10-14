package com.example.processors;

import com.example.models.Power;
import com.example.models.TurbineState;
import com.example.models.Type;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

// Processor<KIn,VIn,KOut,VOut>
/*
KIn - Key input
VIn - Value input
KOut - Key output
VOut - Value output
 */

// An example record
// Key_1|Value_{"timestamp": "2020-11-23T09:02:00.000Z", "wind_speed_mph": 40, "power": "ON", "type": "REPORTED"}

// Purpose of this HighWind processor
/*
- check if turbine WindSpeed > 65 and Power == ON
- if so, then send a SHUTDOWN/power off signal to Turbine

For that
- Fetch the value from the original record
- Forward the origrinal record to all downstream processors
- check original record WindSpeed and Power,
    - if threshold exceeding
      then, clone the original record
      Modify the Type to DESIRED and Power to OFF
      Forward the modified record to all downstream processors
-
 */
// Tag: its a stateless processor
public class HighWindFlatmapProcessor // this is a Stateless STREAM processor created for ProcessorAppTopology
    implements Processor<String, TurbineState, String, TurbineState> { // implements the Processor interface, not functional interface

    private ProcessorContext<String, TurbineState> context;

    // ProcessorContext<KOut, VOut>
    @Override // init() method is called when the Processor is first instantiated
    public void init(ProcessorContext<String, TurbineState> context) {
        //Processor.super.init(context);
        // save the ProcessorContext as instance property, so that we can use it later from process() and close() methods
        this.context = context;
    }


    // Main processing logic
    // Record<K,V>
    /*
    K- type of Key
    V- type of Value
     */
    // An example record
    // Key_1|Value_{"timestamp": "2020-11-23T09:02:00.000Z", "wind_speed_mph": 40, "power": "ON", "type": "REPORTED"}
    @Override // process() method is called whenever it receives a new record. contains per-record data transformation logic
    public void process(Record<String, TurbineState> originalRecord) {
        // Target: Detect whether or not wind speed exceed safe operating levels for our turbine

        TurbineState reportedStateRecordValue = originalRecord.value(); // fetch the record value
        context.forward(originalRecord); // broadcasting
                                        // forward the original "reported-state-events" record to the downstream processor;
                                        // here downstream processor is "digital-twin" STREAM processor

        // Check if conditions meet to send the SHUTDOWN signal to Turbine
        if (reportedStateRecordValue.getWindSpeedMph() > 65 && reportedStateRecordValue.getPower()== Power.ON){
            // clone the existing records with type is "DESIRED" and Power is "OFF"
            TurbineState desired = TurbineState.clone(reportedStateRecordValue);
            desired.setPower(Power.OFF);
            desired.setType(Type.DESIRED);

            // create a new record with this cloned and modified record
            // newRecord = originalRecordKey + desired state value + originalRecordTimestamp
            Record<String, TurbineState> newRecord =
                    new Record<>(originalRecord.key(), desired, originalRecord.timestamp());

            // forward the new record (the shutdown signal) to all the downstram processor
            context.forward(newRecord); // but where this newRecord will be forwarded?
                                        // this is kinda "Broadcast", as broadcasting to all downstream processors

            // if we wants to forward to a specific downstream processor, then explicitly mentioned the Procesor name
            // context.forward(newRecord, "some-child-node");

        }

    }

    @Override
    public void close() {
        //Processor.super.close();
        // nothing to do
    }
}
