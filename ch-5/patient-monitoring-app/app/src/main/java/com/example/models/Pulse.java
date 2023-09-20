package com.example.models;

import lombok.AllArgsConstructor;
import lombok.Data;

/*
pulse-event stream data: these data are populated by HeartBeat Sensors| Keyed records/events
1(patientID)|{"timestamp": "2020-11-23T09:02:00.000Z"}
1|{
    "timestamp": "2020-11-23T09:02:00.500Z" // timestamp is embedded in the payload of the event
    }
 */
@Data
@AllArgsConstructor
public class Pulse implements Vital{
    private String timestamp;

    @Override // will be used at 'VitalTimestampExtractor' for our 'event-time-semantics'
    public String getTimeStamp() {
        return this.timestamp;
    }
}

// we need to convert these raw pulse events/records into a heart rate (bpm - measured using beats per minute)
// Group by clause: Grouping {Pulse, BodyTemp}
