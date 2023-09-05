package com.example;

import com.example.language.DummyClient;
import com.example.model.EntitySentiment;
import com.example.model.Tweet;
import com.example.serdes.TweetSerde;
import com.example.serdes.serializer.AvroSerde;
import com.mitchseymour.kafka.serialization.avro.AvroSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.List;

public class CryptoTopologyV1 {
    private static final List<String> currencies = Arrays.asList("bitcoin", "ethereum");
    private static final boolean useSchemaRegistry = true;

    public static Topology build() {
        // Add a StreamBuilder to construct a Kafka topology
        StreamsBuilder builder = new StreamsBuilder();

        // A. Source Processor
        KStream<byte[], Tweet> source = builder.stream(
                "tweets",
                Consumed.with(Serdes.ByteArray(), new TweetSerde())); // this will let the Stream populated with Tweet objects instead of byte array

        // B. STREAM PROCESSOR - to print the KStream
        // B1. Filter the Retweets from the downstream processors
        Predicate<byte[], Tweet> retweets = (key, tweet) -> tweet.isRetweet();
        KStream<byte[], Tweet> filtered = source.filterNot(retweets); // it's a function using predicates

        // B2. Branching Stream
        // Separating our filtered stream into sub-streams based on the source language of the tweet
        Predicate<byte[], Tweet> englishTweets = (key, tweet) -> tweet.getLang().equals("en");
        Predicate<byte[], Tweet> nonEnglishTweets = (key, tweet) -> !tweet.getLang().equals("en");

        // Create branches using this predicates
        var branches = filtered.branch(englishTweets, nonEnglishTweets); // ordering of predicates matter
        var englishStream = branches[0];
        var nonEnglishStream = branches[1];

        // B3. Translating NonEnglish Tweets to English - one to one mapping of records
        // create a new DummyClient instance
        DummyClient languageClient = new DummyClient(); // Dummy Client for translating NonEnglish Tweets to English Tweets.

        // 1:1 relationship
        KStream<byte[], Tweet> translatedStream = nonEnglishStream.mapValues(
                (tweet) -> {
                    //var newKey = tweet.getUser().getName().getBytes();
                    var translatedTweet = languageClient.translate(tweet, "en");
                    return translatedTweet;
                    //return KeyValue.pair(newKey, translatedTweet); // binding the new key and new value together
                });

        // B4. Merge these two streams : englistStream and TranslatedTweetStream into a Single Kafka Stream
        KStream<byte[], Tweet> mergedStream = englishStream.merge(translatedStream);

        //B4. Sentiment Analysis
        // 1:N relationship using Kafka flatMap and flatMapValues
        // Output:  {"entity": "bitcoin", "sentiment_score": 0.80}
        //          {"entity": "ethereum", "sentiment_score": -0.20}
        // definition in src/main/avro/entity_sentiment.avsc
        KStream<byte[], EntitySentiment> sentimentScoredStream = mergedStream.flatMapValues(
                (tweet) -> {
                    List<EntitySentiment> results = languageClient.getEntitySentiment(tweet);
                    results.removeIf(entitySentiment -> !currencies.contains(entitySentiment.getEntity()));
                    //System.out.println(results);
                    return results; // AVRO encode records
                }
        );
        getPrint(sentimentScoredStream, "sentiment-scored-tweet-stream"); // Avro encode records

        // C. SINK PROCESSOR
        // Note: Kafka is a byte-in and bytes-out stream processing platform
        if (useSchemaRegistry) {
            sentimentScoredStream.to(
                    "crypto-sentiment",
                    Produced.with(
                            Serdes.ByteArray(),
                            AvroSerde.EntitySentiment("http://localhost:8081", false)
                    )
            );

        } else {
            sentimentScoredStream.to(
                    "crypto-sentiment",
                    Produced.with(
                            Serdes.ByteArray(),
                            // registryless Avro Serde
                            AvroSerdes.get(EntitySentiment.class)));
        }




        // Return the topology to App.main()
        Topology topology = builder.build();
        return topology;

    }

    private static <K, V> void getPrint(KStream<K, V> kStream, String label) {
        kStream.print(Printed.<K, V>toSysOut().withLabel(label));
    }

}
