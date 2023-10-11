package com.example.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// Data Model-02: Digital Twin
// To combine both reported-state-events.json and desired-state-events.json records into a DigitalTwin record
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
