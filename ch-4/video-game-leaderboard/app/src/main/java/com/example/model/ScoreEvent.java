package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// {"score": 1200.0, "product_id": 6, "player_id": 4}
// For Kafka Topic: "score-events"
// Goes to Deserializer and Serializer
@Data
@AllArgsConstructor
@NoArgsConstructor  // default constructor
public class ScoreEvent {
    // Object attributes
    private Long playerId;
    private Long productId;
    private Double score;
}
