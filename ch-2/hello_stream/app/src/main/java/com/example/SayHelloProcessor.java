package com.example;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.function.Function;

// This is for Processing API Example, not for DSL Example

/*
Processor<Key,Value,OutputKey, OutputValue>
 */
// Here, we used CustomProcessor -> SayHelloProcessor which implement the Processor interface
public class SayHelloProcessor implements Processor<Void,String,Void,Void> {
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
    public void process(Record<Void,String> record) {
        // Business Logic for the transformation
        // create a function to transform the value of the record into upper case
        StringManupulator<String,String> stringManupulator = (demoString)-> demoString.toUpperCase(); // FunctionalInterface //Atomic
        Function<String,Integer> lenCalFunc = (demoString)-> demoString.length(); // Function
        Function<String, String> isTooShort = (demoString)-> {
            return lenCalFunc.apply(demoString) > 10? "Full Length" : "too Short"; //lambda function
        }; // Function

        String formattedString = String.format("(Processor API) Hello, %s. Name Length is: %d(%s) ",
                stringManupulator.apply(record.value()),
                lenCalFunc.apply(record.value()),
                isTooShort.apply(record.value()));
        System.out.println(formattedString);
    }

    // clean up the function
    @Override
    public void close() {
        Processor.super.close();
    }
}
