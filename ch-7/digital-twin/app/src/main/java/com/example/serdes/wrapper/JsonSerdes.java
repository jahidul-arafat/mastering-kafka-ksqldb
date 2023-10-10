package com.example.serdes.wrapper;

import com.example.models.DigitalTwin;
import com.example.models.TurbineState;
import com.example.serdes.deserializer.JsonDeserializer;
import com.example.serdes.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

// its a JSON wrapper for serilization and deserialization
// Deserialization - means converting JSON byte stream into the Java objects
// Here, under the model, we have two Objects: DigitalTwin and TurbineState
// Serialization - means converting the Java objects into JSON byte stream
public class JsonSerdes {
    // Class Methods; can only be called as JsonSerdes.DigitalTwin() or JsonSerdes.TurbineState()
    // wrapper for the TurbineState class
    public static Serde<TurbineState> TurbineState() {
        JsonSerializer<TurbineState> serializer = new JsonSerializer<>();
        JsonDeserializer<TurbineState> deserializer = new JsonDeserializer<>(TurbineState.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    // wrapper for the DigitalTwin class
    public static Serde<DigitalTwin> DigitalTwin() {
        JsonSerializer<DigitalTwin> serializer = new JsonSerializer<>();
        JsonDeserializer<DigitalTwin> deserializer = new JsonDeserializer<>(DigitalTwin.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
