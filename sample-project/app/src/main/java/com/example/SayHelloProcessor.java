package com.example;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

// This is for Processing API Example, not for DSL Example

/*
Processor<Key,Value,OutputKey, OutputValue>
 */
// Here, we used CustomProcessor -> SayHelloProcessor which implement the Processor interface
public class SayHelloProcessor implements Processor<String,String,Void,Void> {
    public static final String APP_TYPE="PAPI";
    // initializing the stream processor
    // ProcessorContext<OutputKey, OutputValue>
    @Override
    public void init(ProcessorContext<Void,Void> context) {
        Processor.super.init(context);
    }

    // applying the stream processing logic to a single record/event read from partition of a topic named 'users'
    // Record<Key,Value>
    // For each record/event
    @Override
    public void process(Record<String,String> record) {
        StreamTransformationLogic.commonBusinessProcessingLogic(record.key(), record.value(), APP_TYPE);
    }

    // clean up function
    @Override
    public void close() {
        Processor.super.close();
    }
}
