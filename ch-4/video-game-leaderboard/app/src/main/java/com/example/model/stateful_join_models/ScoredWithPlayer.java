package com.example.model.stateful_join_models;

import com.example.model.Player;
import com.example.model.ScoreEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// Doesn't go to Deserializer and Serializer
@Data
@AllArgsConstructor
@NoArgsConstructor  // default constructor
public class ScoredWithPlayer {
    private ScoreEvent scoreEvent;
    private Player player;
}
