package com.example.language;

import com.example.model.EntitySentiment;
import com.example.model.Tweet;

import java.util.List;

public interface LanguageClient {
    public Tweet translate(Tweet tweet, String targetLanguage); // need to be implemented
    public List<EntitySentiment> getEntitySentiment(Tweet tweet); // need to be implemented

}
