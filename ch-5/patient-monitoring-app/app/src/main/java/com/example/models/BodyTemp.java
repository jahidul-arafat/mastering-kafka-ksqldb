package com.example.models;

import lombok.AllArgsConstructor;
import lombok.Data;

/*
body-temp-event stream data.
- This data is populated by a wireless body temperature sensor | keyed data
1(patientID)|{
    "timestamp": "2020-11-23T09:03:06.500Z",    // timestamp is embedded in the payload of the event
    "temperature": 101.2,
    "unit": "F"
    }

 */
@Data   // getter, setter and toString methods
@AllArgsConstructor
public class BodyTemp implements Vital{
    private String timestamp;
    private Double temperature;
    private String unit;

    @Override
    public String getTimeStamp() {
        return this.timestamp;
    }
}
