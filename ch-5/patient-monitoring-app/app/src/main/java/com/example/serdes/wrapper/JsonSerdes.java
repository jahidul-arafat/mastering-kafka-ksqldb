package com.example.serdes.wrapper;

import com.example.models.BodyTemp;
import com.example.models.CombinedVitals;
import com.example.models.Pulse;
import com.example.serdes.deserializer.JsonDeserializer;
import com.example.serdes.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerdes {
    // These Serdes i.e. wrapper of serializer and deserializer for each Object Type i.e. Pulse, BodyTemp
    // are defined 'static', means they are Class Method, not an Object Method and can only be accessed via
    // ClassName.MethodName
    // JsonSerdes.Pulse()

    // Class Method-1: Define the Serializer (to bytes[]) and deserializer (from raw bytes-JSON to Object Type-Pulse)
    // ** Note: Kafka is byte-in and byte-out stream
    // Sample Pulse Stream Data:
    // 1(patientID)|{"timestamp": "2020-11-23T09:02:36.000Z"}
    // We need to convert this JSON data into Object-Pluse (deserialized) and then from object-Pulse to Byte[] again (Serialized)
    public static Serde<Pulse> Pulse(){
        // Define the serializer
        JsonSerializer<Pulse> serializer = new JsonSerializer<>(); // converting JavaObject Playerv -> to ByteStream-JSON
        // Define the deserializer
        JsonDeserializer<Pulse> deserializer = new JsonDeserializer<>(Pulse.class); // converting raw bytes-JSON to Object Type-Pulse
        // return the serializer and deserializer
        return Serdes.serdeFrom(serializer, deserializer);
    }

    // Class Method-2: for 'BodyTemp' object to deal with 'body-temp' stream event
    // 1(patientID)|{"timestamp": "2020-11-23T09:03:06.500Z", "temperature": 101.2, "unit": "F"}
    public static Serde<BodyTemp> BodyTemp(){
        JsonSerializer<BodyTemp> serializer = new JsonSerializer<>();
        JsonDeserializer<BodyTemp> deserializer = new JsonDeserializer<>(BodyTemp.class);
        return Serdes.serdeFrom(serializer,deserializer);
    }

    // Class Method-3: for 'CombinedVitals' object which will be written back to SINK topic 'alrets' stream event
    // Event/Records which will be written back -> [vitals-after-join]: 1, CombinedVitals(heartRate=120, bodyTemp=BodyTemp(timestamp=2020-11-23T09:03:06.500Z, temperature=101.2, unit=F))
    public static Serde<CombinedVitals> CombinedVitals(){
        JsonSerializer<CombinedVitals> serializer = new JsonSerializer<>();
        JsonDeserializer<CombinedVitals> deserializer = new JsonDeserializer<>(CombinedVitals.class);
        return Serdes.serdeFrom(serializer,deserializer);
    }
}
