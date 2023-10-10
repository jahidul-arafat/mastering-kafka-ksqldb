package com.example.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// For Step-1: Consuming source event streams at "reported-state-event" topic
// Target Event Stream/DataSet: reported-state-events.json
// 1|{"timestamp": "2020-11-23T09:02:00.000Z", "wind_speed_mph": 40, "power": "ON", "type": "REPORTED"}
@Data // getter, setter and toString
@AllArgsConstructor // default constructor
public class TurbineState {
    // Scenario: There will be 40 turbine and each turbine will repost the below information
    /*
    - reporting time
    - wind speed in mile per hour
    - Power ON/OFF status of turbine | define this as ENUM as there are only two constants
    - REPORTED/DESIRED status of turbine | define this as ENUM as there are only two constants
        - REPORTED means, the current state of the turbine
        - DESIRED means, the desired state of the turbine i.e. from my app, i want to Switch-off the turbine if it detects a dangerous windspeed.
     */
    private String timestamp;
    private Double windSpeedMph;
    private Power power;
    private Type type;

    // Class Method-01: Create a twin/copy of the original TurbineState
    // Note: this would be a class method, means, can only be called with TurbineState.clone()
    public static TurbineState clone(TurbineState original) {
        return new TurbineState(
                original.getTimestamp(),
                original.getWindSpeedMph(),
                original.getPower(),
                original.getType());
    }

}
