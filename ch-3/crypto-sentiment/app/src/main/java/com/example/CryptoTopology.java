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

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CryptoTopology {
    private static final List<String> currencies = Arrays.asList("bitcoin", "ethereum");
    private static final boolean useSchemaRegistry = true; // My Avro schema registry is at http://localhost:8081 ; I am not using the Confluent Online Schema registry here; instead managing my own schema reqistry here

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
                Consumed.with(
                        Serdes.ByteArray(), // Key of the record will be deserialized as ByteArray
                        new TweetSerde()    // Value of the record will be deserialized as a Tweet object
                ));
        // this will let the Stream populated with Tweet objects instead of byte array

        // B. STREAM PROCESSOR - to print the KStream
        // Why Keys are still being serialized using byte[]
        // we will only be deserializing the Values to Tweet objects, keys will still be serialized using byte[] and will not be deserialized
        //source.print(Printed.<byte[],byte[]>toSysOut().withLabel("tweet-stream")); // old implementation
        //getPrint(source, "tweet-stream");

        // B1. Filter the Retweets from the downstream processors
        // Means, we dont want any retweets to be processed by our Kafka Stream Processors
        Predicate<byte[], Tweet> retweets = (key, tweet) -> tweet.isRetweet(); // filter to check if Tweet has Retweet Flag TRUE
        KStream<byte[], Tweet> filtered = source.filterNot(retweets); // get the Stream of Tweets having no Reweets// it's a function using predicates
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
        var englishStream = branches[0]; // kStream<byte[], Tweet> with english tweets
        var nonEnglishStream = branches[1]; // kStream<byte[], Tweet> with non-english tweets
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
                    var translatedTweet = languageClient.translate(tweet, "en"); // return the translated tweet object
                    return translatedTweet;
                    //return KeyValue.pair(newKey, translatedTweet); // binding the new key and new value together
                });
        // print out the KStream
        //getPrint(translatedStream, "translated-tweet-stream"); // new implementation

        // B4. Merge these two streams : englistStream and TranslatedTweetStream into a Single Kafka Stream
        // Merging is opposite to Branching operations
        // Why Merging? To avoid unnecessary code duplication and as same processing logic can be applied to both streams. So, better to merge together
        KStream<byte[], Tweet> mergedStream = englishStream.merge(translatedStream); // merge operation is stateless, unconditional merge
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

        // C. SINK PROCESSOR: write the Kafka stream (sentimentScoredStream) into a Kafka topic named "crypto-sentiment"
        // Note: Kafka is a byte-in and bytes-out stream processing platform
        // Serialization using AVRO before writing to Kafka Topic "crypto-sentiment"

        // Note: Kafka is a byte-in and bytes-out stream processing platform
        // Add a sink processor to the topology to write tweets to the Topic "crypto-sentiment"
        // KStream<K,V> ; KSteam is an interface
        // Now we need to Serialize these Avro encoded sentimentScoreRecords into a new Topic "crypto-sentiment"
        // Using Avro registryless serde, convert the sentimentEnrichedRecord
        // produced by Avro backed "EntitySentiment" class to a byte array

        // We will be using registry-aware Avro Serde
        // Why? For better support for Schema evolution and smaller message sizes



        /*
        Summary:
        - Avro is used for serialization when producing messages to the Kafka topic.
        - Avro is involved in deserialization when consuming messages from the Kafka topic.

        Explanation:
        - Avro is a binary serialization format, and it includes a schema that describes the structure of the data being serialized.
        - In our Kafka Streams application, we've defined a schema for the EntitySentiment data,
        and Avro is being used to serialize instances of this data structure into byte arrays before they are sent to the Kafka topic.

        - On the other side, when consumers read messages from the Kafka topic,
        they need to deserialize the byte arrays back into the original data structures.
        Avro is also involved in the deserialization process,
        as it understands the schema information included in the serialized data and can reconstruct the original objects.
         */

        if (useSchemaRegistry) { // using Avro Schema Registry
            // options:  .to -> if we reached to the end of our Processor Topology
            // .repartition or .through -> if additional operators/stream processing logic is there
            // Here, http://localhost:8081 -> is the URL of the Schema Registry
            // false ->  schema should not be registered with the registry provided by Confluent
            // By using the schema registry, we can ensure that the data is written in the correct format and
            // that the Kafka broker can correctly interpret it.

            /*
            Syntax of AvroSerde.EntitySentiment("http://localhost:8081", false)
            - If isKey is true, it means the Avro Serde is configured for handling key serialization and deserialization.
            - If isKey is false, it means the Avro Serde is configured for handling value serialization and deserialization.

            We Want Avro Serde to handle value serialization and deserialization.
             */
            sentimentScoredStream.to(
                    "crypto-sentiment",
                    Produced.with(
                            Serdes.ByteArray(), // Serdes.ByteArray() is a key serializer provided by Kafka Streams, and it indicates that the key should be serialized as a byte array.
                            AvroSerde.EntitySentiment("http://localhost:8081", false) // means that the data (value) will be serialized into a byte stream using the Avro serialization format before being written to the Kafka topic.
                            // The AvroSerde.EntitySentiment("http://localhost:8081", false) part specifies the Avro serialization for the value.
                            // It means that instances of the EntitySentiment type will be serialized into a byte stream using the Avro serialization format.
                            // The Avro schema and other necessary information are typically managed by a schema registry (in this case, at "http://localhost:8081"),
                            // which allows consumers to deserialize the data properly.
                            // Here, in our case Avro Serde will rely on a schema registry located at the specified URL ("http://localhost:8081") to fetch and manage Avro schemas. In this case, you are specifying "http://localhost:8081" as the schema registry URL.
                    )
            );

        } else {
            // Registry-less // not using Avro Schema Registry
            // Error prone
            // Without the schema registry, the Kafka broker will try to infer the schema from the data,
            // which can lead to errors if the data contains fields that are not part of the EntitySentiment Class.
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
