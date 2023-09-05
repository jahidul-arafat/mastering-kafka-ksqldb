package com.example.model.stateful_join_models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// {player_id, player_name, product_id, game_name, score}
// Goes to Deserializer and Serializer
@Data
@AllArgsConstructor
@NoArgsConstructor  // default constructor
public class EnrichedWithAll implements Comparable<EnrichedWithAll>{
    // Object Attributes
    private Long playerId;
    private String playerName;
    private Long productId;
    private String gameName;
    private Double score;


    // to sort the records by player score to get the top three HighScore
    // for: HighScores class
    @Override
    public int compareTo(EnrichedWithAll o) {
        return Double.compare(o.score, this.score);
    }
}
