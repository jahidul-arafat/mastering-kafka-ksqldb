package com.example.models;

/*
Why this interface?
- Becoz, our "pulse-event" Stream and "body-temp-event" Stream both has timestamp.
- Thats why, we created this Interface which will be implemented by both "Pulse" and "BodyTemp" classes
 */
public interface Vital {
    public String getTimeStamp();
}
