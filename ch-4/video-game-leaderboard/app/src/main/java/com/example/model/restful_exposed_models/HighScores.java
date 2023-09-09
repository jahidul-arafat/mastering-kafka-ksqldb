package com.example.model.restful_exposed_models;

import com.example.model.stateful_join_models.EnrichedWithAll;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

// Goes to Serializer and Deserializer
@Data
@NoArgsConstructor
public class HighScores {
    // [enriched-with-all]: 3 (Player_ID), EnrichedWithAll(playerId=3, playerName=Isabelle, productId=6, gameName=Mario Kart, score=9000.0)
    private final TreeSet<EnrichedWithAll> highScoreSet = new TreeSet<>(); // sorted Set; Sorted by Score // Ordered set

    // cant use HashSet or LinkedHashSet are these are not sorted by score

    // method to add an enriched record into the sorted TreeSet and keep only the top 3 score records
    public HighScores add(final EnrichedWithAll enrichedRecord) {
        highScoreSet.add(enrichedRecord);
        /*
        add 1
        record [1]

        add 2
        record [2,1]

        add 0
        record [2,1,0]

        add 100
        record [100, 2,1,0] --> remove last (0)
        record [100,2,1]
         */


        if (highScoreSet.size() > 3)
            highScoreSet.remove(highScoreSet.last());
        return this; // unconventional adder() method returing this; becoz of the Aggregate function
        // which requires to add an enrichedRecord to the treeSet
    }

    // List all the scored by players
    public List<EnrichedWithAll> toList() {
        return highScoreSet.stream().toList();
    }

    /*
    //old version
      private final TreeSet<Enriched> highScores = new TreeSet<>();

      public HighScores add(final Enriched enriched) {
        highScores.add(enriched);

        // keep only the top 3 high scores
        if (highScores.size() > 3) {
          highScores.remove(highScores.last());
        }

        return this;
      }

      public List<Enriched> toList() {

        Iterator<Enriched> scores = highScores.iterator();
        List<Enriched> playerScores = new ArrayList<>();
        while (scores.hasNext()) {
          playerScores.add(scores.next());
        }

        return playerScores;
      }
     */
}
