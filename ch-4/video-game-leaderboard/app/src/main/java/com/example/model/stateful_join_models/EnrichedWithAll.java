package com.example.model.stateful_join_models;

import com.example.model.Product;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// {player_id, player_name, product_id, game_name, score}
// Goes to Deserializer and Serializer
@Data
public class EnrichedWithAll implements Comparable<EnrichedWithAll>{
    // Object Attributes
    private Long playerId;
    private String playerName;
    private Long productId;
    private String gameName;
    private Double score;

    // Redefined constructor for the KStream-GlobalKTable join operation
    public EnrichedWithAll(ScoredWithPlayer scoreWithPlayer, Product product) {
        this.playerId = scoreWithPlayer.getPlayer().getId();
        this.playerName = scoreWithPlayer.getPlayer().getName();
        this.productId = product.getId();
        this.gameName = product.getName();
        this.score = scoreWithPlayer.getScoreEvent().getScore();
    }


    // to sort the records by player score to get the top three HighScore
    // for: HighScores class
    @Override
    public int compareTo(EnrichedWithAll o) {
        return Double.compare(o.score, this.score);
    }
}
