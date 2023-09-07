package com.example.serdes.wrapper;

import com.example.model.Player;
import com.example.model.Product;
import com.example.model.ScoreEvent;
import com.example.model.restful_exposed_models.HighScores;
import com.example.model.stateful_join_models.EnrichedWithAll;
import com.example.serdes.deserializer.JsonDeserializer;
import com.example.serdes.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerdes {
    // These Serdes i.e. wrapper of serializer and deserializer for each Object Type i.e. Player, Product, ScoreEvent
    // are defined 'static', means they are Class Method, not an Object Method and can only be accessed via
    // ClassName.MethodName
    // JsonSerdes.Player()

    // Class Method-1
    public static Serde<Player> Player() {
        JsonSerializer<Player> serializer = new JsonSerializer<>();
        JsonDeserializer<Player> deserializer = new JsonDeserializer<>(Player.class); // destinationClass -> Player
        // Means, deserialized the rawJsonBytes into a Java Player Object
        return Serdes.serdeFrom(serializer, deserializer);

    }

    // Class Method-2
    public static Serde<Product> Product() {
        JsonSerializer<Product> serializer = new JsonSerializer<>();
        JsonDeserializer<Product> deserializer = new JsonDeserializer<>(Product.class); // destinationClass -> Player
        // Means, deserialized the rawJsonBytes into a Java Player Object
        return Serdes.serdeFrom(serializer, deserializer);

    }

    // Class Method-3
    public static Serde<ScoreEvent> ScoreEvent() {
        JsonSerializer<ScoreEvent> serializer = new JsonSerializer<>(); // serializing the Java ScoreEvent object to byte[] stream
        JsonDeserializer<ScoreEvent> deserializer = new JsonDeserializer<>(ScoreEvent.class); // destinationClass -> Player
        // Means, deserialized the rawJsonBytes into a Java ScoreEvent Object
        return Serdes.serdeFrom(serializer, deserializer);

    }

    // Class Method-4
    public static Serde<EnrichedWithAll> EnrichedWithAll() {
        JsonSerializer<EnrichedWithAll> serializer = new JsonSerializer<>();
        JsonDeserializer<EnrichedWithAll> deserializer = new JsonDeserializer<>(EnrichedWithAll.class); // destinationClass -> Player
        // Means, deserialized the rawJsonBytes into a Java EnrichedWithAll Object
        return Serdes.serdeFrom(serializer, deserializer);

    }

    // Class Method-5
    public static Serde<HighScores> HighScores() {
        JsonSerializer<HighScores> serializer = new JsonSerializer<>();
        JsonDeserializer<HighScores> deserializer = new JsonDeserializer<>(HighScores.class); // destinationClass -> Player
        // Means, deserialized the rawJsonBytes into a Java HighScores Object
        return Serdes.serdeFrom(serializer, deserializer);

    }


}
