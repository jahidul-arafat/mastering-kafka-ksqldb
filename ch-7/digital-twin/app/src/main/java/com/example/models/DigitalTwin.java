package com.example.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// For 3.1
/*
- Each digital twin record will be written to an output topic called digital-twins for analytical purposes.
- In this step, you will learn how to crate athe DigitalTwin object that would be used for Deserlization
- Deserialiozation means- convent the event-stream raw ByteStrewam json data into a Java Object
- Later this data will be serialzied again (convert the Java Object into byte stream JSON data) to write to a SINK processor
 */
@Data // getter, setter and toString
@NoArgsConstructor // default constructor
@AllArgsConstructor // default constructor
public class DigitalTwin {
    // this will accumulate both the reportedState and the desiredState of the turbine
    // Define the object attributes
    private TurbineState reportedState;
    private TurbineState desiredState;
}
