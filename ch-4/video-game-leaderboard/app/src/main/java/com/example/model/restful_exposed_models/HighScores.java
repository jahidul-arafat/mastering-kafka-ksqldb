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
@AllArgsConstructor
@NoArgsConstructor  // default constructor
public class HighScores {
    private TreeSet<EnrichedWithAll> highScoreSet; // sorted Set; Sorted by Score
                                    // cant use HashSet or LinkedHashSet are these are not sorted by score

    // method to add an enriched record into the sorted TreeSet and keep only the top 3 score records
    public void add(EnrichedWithAll enrichedRecord){
        highScoreSet.add(enrichedRecord);
        if (highScoreSet.size()>3)
            highScoreSet.remove(highScoreSet.last());
    }

    // List all the scored by players
    public List<EnrichedWithAll> toList(){
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
