package com.example;

import com.example.language.DummyClient;
import com.example.language.LanguageClient;
import com.example.model.EntitySentiment;
import com.example.model.Tweet;
import com.example.serdes.TweetSerde;
import com.example.serdes.serializer.AvroSerde;
import com.mitchseymour.kafka.serialization.avro.AvroSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.List;

public class CryptoTopology {
    private static final List<String> currencies = Arrays.asList("bitcoin", "ethereum");
    private static final boolean useSchemaRegistry = true;

    // Why static? Because App.main() is a static method and its gonna call this Topology.build() method
    public static Topology build() {
        // Add a StreamBuilder to construct a Kafka topology
        StreamsBuilder builder = new StreamsBuilder();

        // A. Source Processor
        // Add a source processor to the topology to read tweets from the Topic "tweets"
        // KStream<Key,Value> ; KSteam is an interface
        // KStream<byte[],byte[]>, means record/events keys and values coming out of the topic "tweets" are being encoded as byte arrays
        // But tweet records are actually encoded as JSON object by the source connector; then what gives these bytes?
        // Bcoz, Kafka Streams, by defaults, represent data flows through our application as byte arrays; and its a sensible approach as this doesnt impose any particular data format on its client
        // and also fast(required less memory and CPU cycles on the brokers to transfer a raw byte stream over the network)
        // So, Who will serialize or deserailzie these byte streams?
        // Kafka client, including kafka stream applications are responsible to do so to work with high-level objects or formats i.e. Strings, JSON, Avro etc

        //KStream<byte[], byte[]> source = builder.stream("tweets"); // we just consumed the byte stream

        // using Custom Serde class - as serialization/deserialization is ready for use by the STREAM PROCESSOR
        // Lets consume from the Topic 'Tweets' using Custom Serde class named "TweetSerde" for serialization and deserialization
        // KStream<K,V>
        // Why Keys are still being serialized using byte[]
        // we will only be deserializing the Values to Tweet objects, keys will still be serialized using byte[]
        // and will not be deserialized

        KStream<byte[], Tweet> source = builder.stream(
                "tweets",
                Consumed.with(Serdes.ByteArray(), new TweetSerde())); // this will let the Stream populated with Tweet objects instead of byte array

        // B. STREAM PROCESSOR - to print the KStream
        // Why Keys are still being serialized using byte[]
        // we will only be deserializing the Values to Tweet objects, keys will still be serialized using byte[] and will not be deserialized
        //source.print(Printed.<byte[],byte[]>toSysOut().withLabel("tweet-stream")); // old implementation
        //getPrint(source, "tweet-stream");

        // B1. Filter the Retweets from the downstream processors
        // Means, we dont want any retweets to be processed by our Kafka Stream Processors
        Predicate<byte[], Tweet> retweets = (key, tweet) -> tweet.isRetweet();
        KStream<byte[], Tweet> filtered = source.filterNot(retweets); // it's a function using predicates
        //getPrint(filtered,"filtered-stream"); // new implementation

        // B2. Branching Stream
        // Separating our filtered stream into sub-streams based on the source language of the tweet
        // Lets create a Predicate to filter the tweets based on the source language of the tweet
        Predicate<byte[], Tweet> englishTweets = (key, tweet) -> tweet.getLang().equals("en");
        Predicate<byte[], Tweet> nonEnglishTweets = (key, tweet) -> !tweet.getLang().equals("en");

        // create a function using this predicate
        //KStream<byte[], Tweet> englishTweetsStream = filtered.filter(englishTweets);
        //englishTweetsStream.print(Printed.<byte[],Tweet>toSysOut().withLabel("english-tweet-stream")); // new implementation

        // Create branches using this predicates
        var branches = filtered.branch(englishTweets, nonEnglishTweets); // ordering of predicates matter
        var englishStream = branches[0];
        var nonEnglishStream = branches[1];
        //getPrint(englishStream, "english-tweet-stream"); // new implementation
        //getPrint(nonEnglishStream, "non-english-tweet-stream"); // new implementation

        // B3. Translating NonEnglish Tweets to English - one to one mapping of records
        // create a new DummyClient instance
        DummyClient languageClient = new DummyClient(); // Dummy Client for translating NonEnglish Tweets to English Tweets.

        // 1:1 relationship
        // if only with values, then try .mapValues() operator
        // we dont want to rekey any records/events read and processed from the SOURCE STREAM of a topic partition
        KStream<byte[], Tweet> translatedStream = nonEnglishStream.mapValues(
                (tweet) -> {
                    //var newKey = tweet.getUser().getName().getBytes();
                    var translatedTweet = languageClient.translate(tweet, "en");
                    return translatedTweet;
                    //return KeyValue.pair(newKey, translatedTweet); // binding the new key and new value together
                });
        // print out the KStream
        //getPrint(translatedStream, "translated-tweet-stream"); // new implementation

        // B4. Merge these two streams : englistStream and TranslatedTweetStream into a Single Kafka Stream
        // Merging is opposite to Branching operations
        // Why Merging? To avoid unnecessary code duplication and as same processing logic can be applied to both streams. So, better to merge together
        KStream<byte[], Tweet> mergedStream = englishStream.merge(translatedStream);
        //getPrint(mergedStream, "merged-tweet-stream"); // new implementation

        //B4. Sentiment Analysis
        // 1:N relationship using Kafka flatMap and flatMapValues
        // Why not map?
        // because map() function produces one output for one input value, whereas flatMap() function produces an arbitrary no of values as output
        // Example: #bitcoin is looking super strong. #ethereum has me worried though
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
        // Add a sink processor to the topology to write tweets to the Topic "crypto-sentiment"
        // KStream<K,V> ; KSteam is an interface
        // Now we need to Serialize these Avro encoded sentimentScoreRecords into a new Topic "crypto-sentiment"
        // Using Avro registryless serde, convert the sentimentEnrichedRecord
        // produced by Avro backed "EntitySentiment" class to a byte array

        // We will be using registry-aware Avro Serde
        // Why? For better support for Schema evolution and smaller message sizes
        if (useSchemaRegistry) {
            // options:  .to -> if we reached to the end of our Processor Topology
            // .repartition or .through -> if additional operators/stream processing logic is there
            // Here, http://localhost:8081 -> is the URL of the Schema Registry
            // false ->  schema should not be registered with the registry provided by Confluent
            // By using the schema registry, we can ensure that the data is written in the correct format and
            // that the Kafka broker can correctly interpret it.
            sentimentScoredStream.to(
                    "crypto-sentiment",
                    Produced.with(
                            Serdes.ByteArray(),
                            AvroSerde.EntitySentiment("http://localhost:8081", false)
                    )
            );

        } else {
            // Registry-less
            // Error prone
            // Without the schema registry, the Kafka broker will try to infer the schema from the data,
            // which can lead to errors if the data contains fields that are not part of the schema.
            sentimentScoredStream.to(
                    "crypto-sentiment",
                    Produced.with(
                            Serdes.ByteArray(),
                            // registryless Avro Serde
                            com.mitchseymour.kafka.serialization.avro.AvroSerdes.get(EntitySentiment.class)));
        }




        // Return the topology to App.main()
        Topology topology = builder.build();
        return topology;

    }

    // Generic Type K,V
    // K-> byte[]
    // V-> Tweet/EntitySentiment
    private static <K, V> void getPrint(KStream<K, V> kStream, String label) {
        kStream.print(Printed.<K, V>toSysOut().withLabel(label));
    }

}
