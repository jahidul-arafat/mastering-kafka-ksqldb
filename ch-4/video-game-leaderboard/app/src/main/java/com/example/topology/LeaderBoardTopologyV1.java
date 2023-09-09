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


public class LeaderBoardTopologyV1 {
    // Class Methods

    // Class Method-1| the main method to build a Kafka Topology which returns a Topology
    public static Topology build() {
        // A. StreamBuilder to construct the Kafka Topology
        StreamsBuilder topologyStreamBuilder = new StreamsBuilder();

        // B. Adding 3X SOURCE PROCESSORS:: for 3X topics {score-events, players, products}

        // B1. KStream Abstraction to read from topic "score-events" // KStream<K,V>--> K:: String, V::ScoreEvent JavaObject
        // Rekeying to make is compatible for join operation witn KTable 'players'
        KStream<String, ScoreEvent> scoreEventSource = topologyStreamBuilder
                .stream(
                        "score-events",
                        Consumed.with( // define how records will be consumed
                                Serdes.ByteArray(), // Key of the record will be deserialized as ByteArray
                                JsonSerdes.ScoreEvent() // Value of the record will be deserialized to ScoreEvent Object using JsonSerdes
                        ))
                // repartition with new key 'player_id' // for join operation with KTable 'players' later
                // rekeying ensure related records/events appear on the same partition
                // rekeying require a temporary topic i.e. 'temp-score-events' generated automatically, which is later read by the main Kafka Topic 'score-events'
                // So, Network trip is required, making rekey an expensive operation
                .selectKey((k, v) -> v.getPlayerId().toString()); // rekeying using player_id

        getPrintStream(scoreEventSource, "score-events"); // STREAM PROCESSOR to print only | (Test Only)

        // B2. KTable Abstraction to read from topic "players"
        KTable<String, Player> playerSource = topologyStreamBuilder
                .table(
                        "players",
                        Consumed.with(  // define how records will be consumed
                                Serdes.String(), // Key of the record will be deserialized as String
                                JsonSerdes.Player() // Value of the record will be deserialized to Player Object using JsonSerdes
                        ));
        getPrintKTable(playerSource,"players");



        // B3. GlobalKTable Abstraction to read from topic "products"
        GlobalKTable<String, Product> productSource = topologyStreamBuilder
                .globalTable(
                        "products",
                        Consumed.with(
                                Serdes.String(), // the key '1' or '2' deserialized as String
                                JsonSerdes.Product()    // the value {"id": 1, "name": "Super Smash Bros"} is deserialzied as Product Object using JsonSerdes
                        )
                );


        // C. JOIN Operations
        // C1. KStream-KTable Join:: Joining 'score-events'-> Stream and 'players'-> KTable
        /*
        Input:
        @KStream - 'score-events'
        [score-events]: 4, ScoreEvent(playerId=4, productId=1, score=500.0)
        [score-events]: 1, ScoreEvent(playerId=1, productId=6, score=800.0)

        @KTable - 'players'
        [players] -> 1, Player(id=1, name=Elyse)
        [players] -> 3, Player(id=3, name=Isabelle)

        Expected Output:
        [score-with-players]: 1 (Player_ID), ScoredWithPlayer(scoreEvent=ScoreEvent(playerId=1, productId=1, score=1000.0), player=Player(id=1, name=Elyse))
        [score-with-players]: 3, ScoredWithPlayer(scoreEvent=ScoreEvent(playerId=3, productId=1, score=4000.0), player=Player(id=3, name=Isabelle))
         */

        // C1.1 Define Join Predicate
        // ValueJoiner(input,input, output) // ValueJoiner is a Functional Interface
        ValueJoiner<ScoreEvent,Player, ScoredWithPlayer> predicate_Score_Player_Joiner=
                ScoredWithPlayer::new; // (score,player)-> new ScoredWithPlayer(score,player)
                                        // All argument Constructor Reference // lambda expression // Its a Java Class, not a Functional Interface
                                        // ScoredWithPlayer is not a Functional Interface,
                                        // but it has a Constructor that matches the signature of the 'ValueJoiner' functional interface's 'apply' method

        // C1.2 Define Join Settings
        // Define the Serialization/Deserlialization settings for the keys and values when performing a stream-table join operation
        // Joined<Key, LeftTable, RightTable>
        Joined<String, ScoreEvent,Player> settings_playerJoinParams =
                Joined.with( // for serialization and deserialization
                        Serdes.String(), // Key- PlayerID// indicates the keys in both tables are expected to be deserialized as String from byte stream
                        JsonSerdes.ScoreEvent(), // how to deserialize the ScoreEvent object from binary data (JSON)
                        JsonSerdes.Player()); // how to deserialize the event/json data to Player object

        // C1.3 Perform Actual inner join operation; key at both table should match
        // avoid using Kafka 2.7.0, upgrade to Kafka 3.5.1 with rocksdb 8.3.2
        KStream<String, ScoredWithPlayer> join_ScoreWithPlayer =
                scoreEventSource.join(
                        playerSource,
                        predicate_Score_Player_Joiner, // to create a new ObjectType of 'ScoredWithPlayer'
                        settings_playerJoinParams   // for serialization and deserialization
                );
        getPrintStream(join_ScoreWithPlayer, "score-with-players");
        /*
        Output
        [score-with-players]: 1 (Player_ID), ScoredWithPlayer(scoreEvent=ScoreEvent(playerId=1, productId=1, score=1000.0), player=Player(id=1, name=Elyse))
        [score-with-players]: 3, ScoredWithPlayer(scoreEvent=ScoreEvent(playerId=3, productId=1, score=4000.0), player=Player(id=3, name=Isabelle))
         */


        // C2. KStream-GlobalKTable Join: Joining 'ScoredWithPlayer' and 'Product/Game'
        /*
        Input:
        - ScoredWithPlayer event/record
        [score-with-players]: 1(key_player_id), ScoredWithPlayer(scoreEvent=ScoreEvent(playerId=1, productId=1, score=1000.0), player=Player(id=1, name=Elyse))

        - Product event/record
        1(key_product_id)|{"id": 1, "name": "Super Smash Bros"}


        Expected Output:
        [enriched-with-all]: 3(player_id), EnrichedWithAll(playerId=3, playerName=Isabelle, productId=6, gameName=Mario Kart, score=9000.0)
        */

        // C2.1 Define Joining Strategy | Extract the Key (ProductID) from the ScoredWithPlayer to map these records to a Product
        KeyValueMapper<String, ScoredWithPlayer, String> settings_scoredWithPlayer_Product_keyValueMapper_byProductId =
                (keyPlayerId, scoreWithPlayer)-> { // leftKey --> PlayerId
                    return String.valueOf(scoreWithPlayer.getScoreEvent().getProductId()); // return output key type 'ProductID'
                };



        // C2.2 Joining Predicts, what we are joining and expected output
        // ValueJoiner<input,input,output> // ValueJoiner is a Functional Interface
        ValueJoiner<ScoredWithPlayer,Product,EnrichedWithAll> predicate_ScoreWithPlayer_Product_Joiner=
                EnrichedWithAll::new; // All argument constructor // redefined constructor at EnrichedWithAll class

        // C2.3 Perform the actual join operation
        // latest join stream: join_scoreWithPlayer
        // KStream<key,value>
        KStream<String, EnrichedWithAll> join_ScoreWithPlayer_Product =
                join_ScoreWithPlayer.join( // updated joined KStream // no rekeying // Old Key 'PlayerID' for enrichedStream
                        productSource, // GlobalKTable // key:: ProductID
                        settings_scoredWithPlayer_Product_keyValueMapper_byProductId, // fetched ProductID from the ScoredWithPlayer joined stream to match with the Product joined stream
                        predicate_ScoreWithPlayer_Product_Joiner);
        getPrintStream(join_ScoreWithPlayer_Product, "enriched-with-all");

        // Output: [enriched-with-all]: 4(PlayerID), EnrichedWithAll(playerId=4, playerName=Sammy, productId=6, gameName=Mario Kart, score=1200.0)


        // D. Grouping :: Prerequisite for Aggregation
        // Task: Calculate the high scores of each product id
        /*
        Input:
        [enriched-with-all]: 3(PlayerID), EnrichedWithAll(playerId=3, playerName=Isabelle, productId=6, gameName=Mario Kart, score=9000.0)
        [enriched-with-all]: 4(PlayerID), EnrichedWithAll(playerId=4, playerName=Sammy, productId=1, gameName=Super Smash Bros, score=500.0)
        [enriched-with-all]: 4(PlayerID), EnrichedWithAll(playerId=4, playerName=Sammy, productId=6, gameName=Mario Kart, score=1200.0)

        Expected output:
        [top3-high-scores-per-game] -> 6(Product/GameID), HighScores(highScoreSet=[EnrichedWithAll(playerId=3, playerName=Isabelle, productId=6, gameName=Mario Kart, score=9000.0), EnrichedWithAll(playerId=2, playerName=Mitch, productId=6, gameName=Mario Kart, score=2500.0), EnrichedWithAll(playerId=4, playerName=Sammy, productId=6, gameName=Mario Kart, score=1200.0)])
        [top3-high-scores-per-game] -> 1(Product/GameID), HighScores(highScoreSet=[EnrichedWithAll(playerId=3, playerName=Isabelle, productId=1, gameName=Super Smash Bros, score=4000.0), EnrichedWithAll(playerId=2, playerName=Mitch, productId=1, gameName=Super Smash Bros, score=2000.0), EnrichedWithAll(playerId=1, playerName=Elyse, productId=1, gameName=Super Smash Bros, score=1000.0)])

         */

        // KGroupedStream<Key_ProductID, Value>
        // this is not grouping the records by Product ID
        // Instead its rekeying the records with new key 'Product ID' which was earlier 'Player ID'
        // ++ its defining strategy that this newkey 'Product ID' will be used for grouping when performing 'aggregation' later
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


        // E. Aggregation// to convert the KGroupedStream to a KStream or KTable
        // Calculate the top 3 high scores per ProductID/Game
        /*
        Expected Output:
        [top3-high-scores-per-game] -> 6, HighScores(
            highScoreSet=[
                EnrichedWithAll(playerId=3, playerName=Isabelle, productId=6, gameName=Mario Kart, score=9000.0),
                EnrichedWithAll(playerId=2, playerName=Mitch, productId=6, gameName=Mario Kart, score=2500.0),
                EnrichedWithAll(playerId=4, playerName=Sammy, productId=6, gameName=Mario Kart, score=1200.0)])
        [top3-high-scores-per-game] -> 1, HighScores(
            highScoreSet=[
                EnrichedWithAll(playerId=3, playerName=Isabelle, productId=1, gameName=Super Smash Bros, score=4000.0),
                EnrichedWithAll(playerId=2, playerName=Mitch, productId=1, gameName=Super Smash Bros, score=2000.0),
                EnrichedWithAll(playerId=1, playerName=Elyse, productId=1, gameName=Super Smash Bros, score=1000.0)])
         */

        // E1. Stream Aggregation
        // HighScores java class -> to store new aggregated values // initializer - Functional Interface // lambda implementation
        // Add subsequent new records through 'adder' as arrives

        // E1.1 Setup_01: Initializer function/lambda
        // Tell Kafka to initialize the HighScores class
        Initializer<HighScores> highScoresInitializer = HighScores::new; // lambda

        // E1.2 Setup_02: Adder Function
        // Key-> Product ID, From: EnrichedWithAll, To: HighScores//ReturnType
        Aggregator<String,EnrichedWithAll,HighScores> highScoresAdder =
                (key,enrichedValue,highScoresAggregate)-> highScoresAggregate.add(enrichedValue); // aggregation logic defined in the .add() method
                                                                        // lambda implementation

        // E1.3 Perform the actual aggregation and store the aggregated results as a materialized KTable (State-Store)
        // KTable -> Maintain a Changelog of latest high scores for stateful processing
        KTable<String,HighScores> top3HighScoresPerGame =
                setting_for_groupByAggregation_ProductId_on_EnrichedWithAll.aggregate( // .aggregate() to combine multiple records of same key into a single result
                        highScoresInitializer, // records will be stored sorted in a TreeSet
                        highScoresAdder,
                        Materialized.<String, HighScores, KeyValueStore<Bytes, byte[]>>
                                as("top3-high-scores-per-game-state-store") // 'state-store' name
                                .withKeySerde(Serdes.String()) // key of Serde at State-Store// for serialization and deserialization of key (product/GameID)
                                .withValueSerde(JsonSerdes.HighScores() // value of Serde at State-Store// for serialization and deserialization of value (HighScores JavaObject)
                                )
                );
        getPrintKTable(top3HighScoresPerGame, "top3-high-scores-per-game");




        // Return the topology to App.main()
        Topology topology = topologyStreamBuilder.build();
        return topology;
    }

    private static void getPrintKGroupedStream(KGroupedStream<String, EnrichedWithAll> groupByProductId_on_EnrichedWithAll) {

        System.out.println("\n-----Printing KGroupedStream-----"); // this will not be pritned in stream

        // Note: By default, KTables store only the latest value for each key,
        // so you won't be able to retrieve all historical values for a key directly from the KTable.
        KTable<String, EnrichedWithAll> tableFromStream = groupByProductId_on_EnrichedWithAll.aggregate(
                // Initializer - this initializes the aggregation state
                () -> null, // You can use a more appropriate initializer if needed

                // Aggregator - this function accumulates values for the same key
                (key, value, aggregate) -> value,

                // Materialized - specify how the result should be materialized, including Serdes
                Materialized.<String, EnrichedWithAll, KeyValueStore<Bytes, byte[]>>
                                as("kgrouped-stream-intemidiary-state-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerdes.EnrichedWithAll())
        );

        getPrintKTable(tableFromStream, "kgrouped-stream-intemidiary"); // only publish the latest record, not all as using reduce

    }


    // Generic Type K,V
    // K-> byte[]
    // V-> Tweet/EntitySentiment
    private static <K, V> void getPrintStream(KStream<K, V> kStream, String label) {
        kStream.print(Printed.<K, V>toSysOut().withLabel(label));
    }

    // method to print a kTable
    private static <K, V> void getPrintKTable(KTable<K, V> kTable, String label) {
        kTable.toStream()
                .foreach((k, v) -> System.out.printf("[%s] -> %s, %s\n",label, k, v));
    }



}


