package com.example.topology;

import com.example.model.Player;
import com.example.model.Product;
import com.example.model.ScoreEvent;
import com.example.model.restful_exposed_models.HighScores;
import com.example.model.stateful_join_models.EnrichedWithAll;
import com.example.model.stateful_join_models.ScoredWithPlayer;
import com.example.serdes.wrapper.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

public class LeaderBoardTopologyNoComment {
    public static Topology build() {
        StreamsBuilder topologyStreamBuilder = new StreamsBuilder();

        KStream<String, ScoreEvent> scoreEventSource = topologyStreamBuilder
                .stream(
                        "score-events",
                        Consumed.with( // define how records will be consumed
                                Serdes.ByteArray(), // Key of the record will be deserialized as ByteArray
                                JsonSerdes.ScoreEvent() // Value of the record will be deserialized to ScoreEvent Object using JsonSerdes
                        ))
                .selectKey((k, v) -> v.getPlayerId().toString()); // rekeying using player_id

        getPrintStream(scoreEventSource, "score-events"); // STREAM PROCESSOR to print only | (Test Only)

        KTable<String, Player> playerSource = topologyStreamBuilder
                .table(
                        "players",
                        Consumed.with(  // define how records will be consumed
                                Serdes.String(), // Key of the record will be deserialized as String
                                JsonSerdes.Player() // Value of the record will be deserialized to Player Object using JsonSerdes
                        ));
        getPrintKTable(playerSource,"players");

        GlobalKTable<String, Product> productSource = topologyStreamBuilder
                .globalTable(
                        "products",
                        Consumed.with(
                                Serdes.String(), // the key '1' or '2' deserialized as String
                                JsonSerdes.Product()    // the value {"id": 1, "name": "Super Smash Bros"} is deserialzied as Product Object using JsonSerdes
                        )
                );

        // JOIN-01: KStream-KTable
        ValueJoiner<ScoreEvent,Player,ScoredWithPlayer> predicate_Score_Player_Joiner=
                ScoredWithPlayer::new; // (score,player)-> new ScoredWithPlayer(score,player)
                                        // All argument Constructor Reference // lambda expression // Its a Java Class, not a Functional Interface
                                        // ScoredWithPlayer is not a Functional Interface,
                                        // but it has a Constructor that matches the signature of the 'ValueJoiner' functional interface's 'apply' method

        Joined<String, ScoreEvent,Player> settings_playerJoinParams =
                Joined.with( // for serialization and deserialization
                        Serdes.String(), // Key- PlayerID// indicates the keys in both tables are expected to be deserialized as String from byte stream
                        JsonSerdes.ScoreEvent(), // how to deserialize the ScoreEvent object from binary data (JSON)
                        JsonSerdes.Player()); // how to deserialize the event/json data to Player object

        KStream<String, ScoredWithPlayer> join_ScoreWithPlayer =
                scoreEventSource.join(
                        playerSource,
                        predicate_Score_Player_Joiner, // to create a new ObjectType of 'ScoredWithPlayer'
                        settings_playerJoinParams   // for serialization and deserialization
                );
        getPrintStream(join_ScoreWithPlayer, "score-with-players");

        // JOIN-02: KStream-GlobalKTable
        KeyValueMapper<String, ScoredWithPlayer, String> settings_scoredWithPlayer_Product_keyValueMapper =
                (keyPlayerId, scoreWithPlayer)-> { // leftKey --> PlayerId
                    return String.valueOf(scoreWithPlayer.getScoreEvent().getProductId()); // return output key type 'ProductID'
                };

        ValueJoiner<ScoredWithPlayer,Product,EnrichedWithAll> predicate_ScoreWithPlayer_Product_Joiner=
                EnrichedWithAll::new; // All argument constructor // redefined constructor at EnrichedWithAll class

        KStream<String, EnrichedWithAll> join_ScoreWithPlayer_Product =
                join_ScoreWithPlayer.join( // updated joined KStream // no rekeying // Old Key 'PlayerID' for enrichedStream
                        productSource, // GlobalKTable // key:: ProductID
                        settings_scoredWithPlayer_Product_keyValueMapper, // fetched ProductID from the ScoredWithPlayer joined stream to match with the Product joined stream
                        predicate_ScoreWithPlayer_Product_Joiner);
        getPrintStream(join_ScoreWithPlayer_Product, "enriched-with-all");


        // Group by productID - prerequisite for Aggregation using KGroupedStream
        KGroupedStream<String,EnrichedWithAll> setting_for_groupByAggregation_ProductId_on_EnrichedWithAll =
                join_ScoreWithPlayer_Product.groupBy(
                        (key, value) -> value.getProductId().toString(), // lambda expression as key selector
                                                                        // key- this key will be used for grouping
                                                                        // new Key of Byte Stream converted to String
                        // group by productId // rekey //repartition // network call
                        Grouped.with( // for serialization and deserialization keys and values after grouping with new key ProductID
                                Serdes.String(), // to convert the byte stream Key_ProductID into a String
                                JsonSerdes.EnrichedWithAll()) // value // group with JsonSerdes.EnrichedWithAll() for serialization and deserialization
                );

        // printing this intermediary
        getPrintKGroupedStream(setting_for_groupByAggregation_ProductId_on_EnrichedWithAll);

        // Aggregation to a KTable and storing in a custom 'state-store'
        Initializer<HighScores> highScoresInitializer = HighScores::new; // lambda

        Aggregator<String,EnrichedWithAll,HighScores> highScoresAdder =
                (key,enrichedValue,highScoresAggregate)-> highScoresAggregate.add(enrichedValue); // aggregation logic defined in the .add() method
                                                                        // lambda implementation
        KTable<String,HighScores> top3HighScoresPerGame =
                setting_for_groupByAggregation_ProductId_on_EnrichedWithAll.aggregate( // .aggregate() to combine multiple records of same key into a single result
                        highScoresInitializer, // records will be stored sorted in a TreeSet
                        highScoresAdder, // aggregation logic defined here
                        Materialized.<String, HighScores, KeyValueStore<Bytes, byte[]>>
                                as("top3-high-scores-per-game-state-store")
                                .withKeySerde(Serdes.String()) // key of Serde at State-Store// for serialization and deserialization of key (product/GameID)
                                .withValueSerde(JsonSerdes.HighScores() // value of Serde at State-Store// for serialization and deserialization of value (HighScores JavaObject)
                                )
                );

        getPrintKTable(top3HighScoresPerGame, "top3-high-scores-per-game");

        top3HighScoresPerGame.toStream().to("high-scores");

        Topology topology = topologyStreamBuilder.build();
        return topology;
    }

    private static void getPrintKGroupedStream(KGroupedStream<String, EnrichedWithAll> groupByProductId_on_EnrichedWithAll) {



        System.out.println("\n-----Printing KGroupedStream-----"); // this will not be pritned in stream
        KTable<String, EnrichedWithAll> tableFromStream = groupByProductId_on_EnrichedWithAll.aggregate(
                () -> null, // You can use a more appropriate initializer if needed
                (key, value, aggregate) -> value,
                Materialized.<String, EnrichedWithAll, KeyValueStore<Bytes, byte[]>>
                                as("kgrouped-stream-intemidiary-state-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerdes.EnrichedWithAll())
        );

        getPrintKTable(tableFromStream, "kgrouped-stream-intemidiary"); // only publish the latest record, not all as using reduce

    }

    private static <K, V> void getPrintStream(KStream<K, V> kStream, String label) {
        kStream.print(Printed.<K, V>toSysOut().withLabel(label));
    }

    private static <K, V> void getPrintKTable(KTable<K, V> kTable, String label) {
        kTable.toStream()
                .foreach((k, v) -> System.out.printf("[%s] -> %s, %s\n",label, k, v));
    }
}


