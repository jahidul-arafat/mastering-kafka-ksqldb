package com.example.language;

import com.example.model.EntitySentiment;
import com.example.model.Tweet;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class DummyClient implements LanguageClient {
    @Override
    public Tweet translate(Tweet tweet, String targetLanguage) {
        tweet.setText("Translated: " + tweet.getText());
        tweet.setLang(targetLanguage);
        return tweet;
    }

    @Override
    public List<EntitySentiment> getEntitySentiment(Tweet tweet) {
        List<EntitySentiment> entitySentimentList = new ArrayList<>();
        Iterable<String> words = Splitter.on(' ')
                .split(tweet.getText().toLowerCase().replace("#", ""));
        for (String word : words) {
            EntitySentiment entitySentiment = EntitySentiment.newBuilder()
                    .setCreatedAt(tweet.getCreatedAt())
                    .setId(tweet.getId())
                    .setEntity(word)
                    .setText(tweet.getText())
                    .setSalience(randomDouble())
                    .setSentimentScore(randomDouble())
                    .setSentimentMagnitude(randomDouble())
                    .build();
            entitySentimentList.add(entitySentiment);
        }
        return entitySentimentList;
    }

    Double randomDouble() {
        return ThreadLocalRandom.current().nextDouble(0, 1);
    }
}
