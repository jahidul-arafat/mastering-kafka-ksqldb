package com.example.model.stateful_join_models;

import com.example.model.Product;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// {player_id, player_name, product_id, game_name, score}
// [enriched-with-all]: 3 (Player_ID), EnrichedWithAll(playerId=3, playerName=Isabelle, productId=6, gameName=Mario Kart, score=9000.0)
// Goes to Deserializer and Serializer
// Why it implements Comparable interface? - to sort the records by score
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
    // Because we will aggregate the enrichedRecords to HighScores class
    // and this aggregation requires the enrichRecords sorted by the score
    // that's why we implemented this Comparable interface
    @Override
    public int compareTo(EnrichedWithAll o) {
        return Double.compare(o.score, this.score);
    }
}
