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
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.ArrayList;
import java.util.List;


/*
When to use which Abstraction?
A. KStream / stored the highest score of each player
    -> If event/record is unkeyed
    -> If event/record is need to store other than the latest state, i.e storing the highest score for each player instead of most recent score
    -> Repartitioning may be applied after being Keyed

B. KTable / Number of players in the game will grow
// partition the state (i.e. players) across multiple application instances
// as storing the state full to all application instances will be overwhelming
    -> If event/record is keyed
    -> If keyspace is very large/or will grow significantly i.e. huge number of unique keys(i.e. player_id) in the records/events :: Reduce local storage overhead for individual KStream instances
    -> KTable is time synchronized::
        -> i.e. reading from multiple topic/sources for JOIN operation (for predictable Join Operations),
        -> Then timestamp is important to determine which record/event to process/consume next

C. GlobalKTable  // Small number of games to play // For small and static data (data that doesnt grow)
// Replicate the states in full across all application instances as states are small in number
// No state partitioning required
    -> If event/record is keyed
    -> if keyspace is not large i.e. not having too many unique keys in the records/events
    -> GlobalKTable is NOT time synchronized:: and populated even before processing is done
 */

public class LeaderBoardTopology {
    // Class Methods

    // Class Method-1| the main method to build a Kafka Topology which returns a Topology
    public static Topology build() {
        // A. StreamBuilder to construct the Kafka Topology
        StreamsBuilder topologyStreamBuilder = new StreamsBuilder();

        // B. Adding 3X SOURCE PROCESSORS:: for 3X topics {score-events, players, products}

        // B1. KStream Abstraction to read from topic "score-events" // KStream<K,V>--> K:: String, V::ScoreEvent JavaObject
        // Note: KStream<K,V> abstraction is used for STATELESS record stream
        // Why KStream for Topic "score-events"?
        // Cause 01: Have a look at the JSON Event/Records we will consume for this topic
        /*
        // {"score": 1200.0, "product_id": 6, "player_id": 4}    --> See this is unkeyed and distributed in a round-robin fashion across the topic's partition

        - This topic "score-events" has 4 partitions and the records/events will be distributed in round-robin fashion in those 4 partitions
        Topic: score-events     PartitionCount: 4       ReplicationFactor: 1    Configs:
            Topic: score-events     Partition: 0    Leader: 1       Replicas: 1     Isr: 1
            Topic: score-events     Partition: 1    Leader: 1       Replicas: 1     Isr: 1
            Topic: score-events     Partition: 2    Leader: 1       Replicas: 1     Isr: 1
            Topic: score-events     Partition: 3    Leader: 1       Replicas: 1     Isr: 1

         // Cause 02: Our application cares about the highest score of each player, not the latest score
         - So the table semantics (retaining only the most recent score for a given key/player) doesn't work
         - That's why we need to use KStream abstraction instead of KTable abstraction
         */

        // KStream<k,v>: k-> not required, v-> ScoreEvent JavaObject
        // Without Re-keying: Not suitable for join operation with KTable 'players'
//        KStream<byte[], ScoreEvent> scoreEventSource = topologyStreamBuilder
//                .stream(
//                        "score-events",
//                        Consumed.with( // define how records will be consumed
//                                Serdes.ByteArray(), // Key of the record will be deserialized as ByteArray
//                                JsonSerdes.ScoreEvent() // Value of the record will be deserialized to ScoreEvent Object using JsonSerdes
//                        ));
//        getPrintStream(scoreEventSource, "score-events"); // STREAM PROCESSOR to print only | (Test Only)

        // Rekeying to make is compatible for join operation with KTable 'players'
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
        // partitioned across all application instances // time sync
        /*
            1|{"id": 1, "name": "Elyse"}        --> Keyed by Player ID
            - We only care about the latest state of the player i.e. the playerName and his/her id
         */
        // key-> String, value-> Player JavaObject
        // Create a partitioned (or sharded) table for the players topic, using the KTable abstraction.
        KTable<String, Player> playerSource = topologyStreamBuilder
                .table(
                        "players",
                        Consumed.with(  // define how records will be consumed
                                Serdes.String(), // Key of the record will be deserialized as String
                                JsonSerdes.Player() // Value of the record will be deserialized to Player Object using JsonSerdes
                        ));
        getPrintKTable(playerSource,"players");



        // B3. GlobalKTable Abstraction to read from topic "products" // small and static // full clone of records to all application instances
        // Create a GlobalKTable for the products topic, which will be replicated in full to each application instance.
        /*
            1|{"id": 1, "name": "Super Smash Bros"} --> Keyed By Product/Game ID
            - We only care about the latest state of the product i.e. the productName and its id
         */

        GlobalKTable<String, Product> productSource = topologyStreamBuilder
                .globalTable(
                        "products",
                        Consumed.with(
                                Serdes.String(), // the key '1' or '2' deserialized as String
                                JsonSerdes.Product()    // the value {"id": 1, "name": "Super Smash Bros"} is deserialzied as Product Object using JsonSerdes
                        )
                );


        // C. JOIN Operations
        /*
        Kafka Join Types
        - Join -> inner join. Left.join(right)-> join trigger, if both Left and Right share the same key

        - Left Join. Left.join(right)-> if key@left --> dont match--> key@Right=> then) ||
                                        if ket@right --> dont match --> key@Left-> not output produced
                                        Both Left and Right can trigger lookup
        - Outer Join.   Left.join(right)
                        Right.join(left)
         */

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

        // Constraints: Observability Issues; if related events are processed by different tasks. Joining would be incorrect or fail
        // A Task is assigned to a Kafka Partition as an observer to handle the consume and produce events

        // Solution Strategy: Route Related events/records to same partition, so handled by the same task
        // Solution: Use co-partitioning
        /*
        Co-partitioning Prerequisites:
            - Topic 'score-events' and 'players' should have the same number of partitions
            - Records on both topics must be keyed by the same field.
                - KStream 'score-events' is unkeyed and required re-keying using player_id
                - KTable 'players' is keyed by player_id
         */


        // Inner join: if both Left (score-events) and Right (players) shares the same key
        /*
        Example:
        KStream<String, ScoreEvent> scoreEvents = ...;  // Left side of join
        KTable<String, Player> players = ...;           // Right side of join

        scoreEvents.join(players, ...);                 // right side is always passed as a parameter to the join operation
         */

        // C1.1 Define Join Predicate
        // Joining 'score-events'-> Stream and 'players'-> KTable and returns a new Java Object 'ScoredWithPlayer'
        // Using 'ValueJoiner', takes two inputs: ScoreEvent and Player; returns 'ScoredWithPlayer' Java Object
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
        // KStream<Key,Value>
        // This join triggers error in Mac M1
        // rocksDB does not support Apple Silicon natively yet.
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

        /*
        At this point, if we look at the kafka topics, we get something like below
        __consumer_offsets  // keeps track of which event/record from each topic is read by consumer
                            // here kafka stores the position of the last consumerd message for each partition of each topic
                            // kafka uses this to store metadata of the 'consumer-group'
                            // in the case consumer (my Java Application) crashes or new consumer joins a group,
                            this determine where each consumer should resume reading from

        dev1-KSTREAM-KEY-SELECT-0000000001-repartition  // automatically created by Join operation as a temporary placeholder
                                                        //for the join output before streaming back to the 'score-events' topic
        dev1-players-STATE-STORE-0000000003-changelog   // for fault tolerance, event states are stored here.
                                                        // in the case of failure, event state stores can be restored by replying the individual events from the underlying changelog topic to reconstruct the state of the application
        high-scores
        players
        products
        score-events

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


        /*
        Preflight Checklist:
        - GlobalTable(Product:: Key ProductID) and KStream(ScoredWithPlayer:: Key PlayerID) need not to share the same key
        - Unlike, KTable (where events/records are partitioned across all application instances),
        GlobalKTable (where events/recrods are full replicated across all application instances)
        - Means local task observing a topic partition has the full copy of the GlobalKTable records
         */

        // Then how to perform the KStream-GlobalKTable join operation ?
        // Solution Strategy: KeyValueMapper | how to map a KStream record/event to a GlobalKTable record/event?

        /*
        Solution Step:

        ** See, both event/records have different keys. But for mapping, we need to say that map with the Product_ID.
        - Extract the ProductID from the ScoredWithPlayer to map these records to a Product
         */

        // C2.1 Define Joining Strategy | Extract the Key (ProductID) from the ScoredWithPlayer to map these records to a Product
        /* keyValueMapper(
            String:: Key :: PlayerID -> of ScoredWithPlayer
            ScoredWithPlayer:: Value associated with the PlayerID i.e.
                            ScoredWithPlayer(scoreEvent=ScoreEvent(playerId=1, productId=1, score=1000.0), player=Player(id=1, name=Elyse))
            String:: new_key:: ProductID -> to be extracted from the ScoredWithPlayer joined stream
            )
         */

        // KeyValueMapper -> is used to extract keys for the join.
        // Target: Extract the new key 'ProductID' from the ScoredWithPlayer joined stream
        // Get the productId from the ScoredWithPlayer joined stream
        // KeyValueMapper - is a function interface having only one abstract method called 'apply'
        // [enriched-with-all]: 3(player_id), EnrichedWithAll(playerId=3, playerName=Isabelle, productId=6, gameName=Mario Kart, score=9000.0)
        KeyValueMapper<String, ScoredWithPlayer, String> settings_scoredWithPlayer_Product_keyValueMapper =
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
                        settings_scoredWithPlayer_Product_keyValueMapper, // fetched ProductID from the ScoredWithPlayer joined stream to match with the Product joined stream
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
        //  Grouping the enriched records to perform aggregation
        /*
        Why Grouping?
        - Same to rekeying
        - to make sure the related records are in the same partition processed by the same task observer

        How is Grouping done?
        - groupBy - similar to selectKey, as key changing operation, required repartitioning. Repartitining is costly, network call required
        - groupByKey - no repartition needed. Not costly as no repartition, thereby no network call required.
         */
        // Which grouping strategy will we apply?
        /*
        - Since enrichedStream using Key:: PlayerID not by ProductID,
        when required top 3 high scores per ProductID,
        we need to repartition the enrichedStream grouping by ProductID(higher_score_1, higher_score_2, higher_score_3)
        - So we will be using 'groupBy' - rekey, repartition and network call
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

        /*
        - aggregate // output type could be different than the input type // can be applied on both streams and tables
                    // KStreams are immutable // initializer + adder
                    // KTables are mutable // initializer + adder + subtractor
        - reduce // input and output type must be same
        - count
         */

        // E1. Stream Aggregation
        // HighScores java class -> to store new aggregated values // initializer - Functional Interface // lambda implementation
        // Add subsequent new records through 'adder' as arrives

        // E1.1 Setup_01: Initializer function/lambda
        // Tell Kafka to initialize the HighScores class
        Initializer<HighScores> highScoresInitializer = HighScores::new; // lambda

        // E1.2 Setup_02: Adder Function
        // Define the logic to combine two aggregates: EnrichWithAll and HighScores
        // Aggregating the EnrichWithALll records to HighScores
        // Use Aggregate - Functional Interface // lambda implementation
        // Key-> Product ID, From: EnrichedWithAll, To: HighScores//ReturnType
        Aggregator<String,EnrichedWithAll,HighScores> highScoresAdder =
                (key,enrichedValue,highScoresAggregate)-> highScoresAggregate.add(enrichedValue); // aggregation logic defined in the .add() method
                                                                        // lambda implementation

        // E1.3 Perform the actual aggregation and store the aggregated results as a materialized KTable (State-Store)
        // Aggregation would return a Table, since aggregation outcomes are immutable
        // KTable<ProductID, HighScores JavaObject>
        // Aggregation on group 'groupByProductId_on_EnrichedWithAll' which is a KGroupedStream - an intermediary
        // Why KTable, why Not KStream ?
        // Here groupByProductId_on_EnrichedWithAll is Keyed by 'ProductID'm KStream streams will be unkeyed; Doesnt mantain a Changelog
        // KTable -> Maintain a Changelog of latest high scores for stateful processing
        KTable<String,HighScores> top3HighScoresPerGame =
                setting_for_groupByAggregation_ProductId_on_EnrichedWithAll.aggregate( // .aggregate() to combine multiple records of same key into a single result
                        highScoresInitializer, // records will be stored sorted in a TreeSet
                        highScoresAdder, // aggregation logic defined here
                        // How the results of this aggregation should be materialized and stored in a state store
                        // check the kafka topic list at this point !!!!
                        // But why do we need a state-Store here?? - (a) to query later (b) to retrieve aggregated data efficiently
                        // We will be doing real-time aggregation to get the Top3 scores of each game at LiveDashBoard
                        // This is stateful as the program need to remember the previously processed data
                        /*
                        Materialized<String, HighScores, KeyValueStore<Bytes, byte[]>>
                        - Materialzied - an interface
                        - <String::Key, HighScores::Value> of the KTable
                        - KeyValueStore<Bytes, byte[]> - Type of State-Store will be created and associated with the KTable
                         */
                        Materialized.<String, HighScores, KeyValueStore<Bytes, byte[]>>
                                as("top3-high-scores-per-game-state-store") // 'state-store' name
                                                    // explicit name of the store for querying outside of the processor topology

                                // customizing the State-store with custom Serdes for Serialization and Deserialization
                                .withKeySerde(Serdes.String()) // key of Serde at State-Store// for serialization and deserialization of key (product/GameID)
                                .withValueSerde(JsonSerdes.HighScores() // value of Serde at State-Store// for serialization and deserialization of value (HighScores JavaObject)
                                )  // once materialized, we will expose this state-store to external word via adhoc queries
                                    // We need to access this state-store in a read-only mode // for that, we need a wrapper
                                    // Wrapping would be done using: QueryableStoreTypes.keyValueStore()
                                    // QueryableStoreTypes is a factory class
                );

        // print this aggregated KTable results
        getPrintKTable(top3HighScoresPerGame, "top3-high-scores-per-game");

        // F. Using KSource Connector
        // Writing back data to Kafka Topic
        // stream this aggregated results to topic 'high-scores'
        top3HighScoresPerGame.toStream().to("high-scores");


        // Implementation - @Main
        // G. Query the State-Store from External Word - this should not be here
        // this should be outside of Kafka Processor Topology
        // I implemented this in main 'App'
        // Should be exposed through a REST API at port localhost:8080/




        // Return the topology to App.main()
        Topology topology = topologyStreamBuilder.build();
        return topology;
    }

    private static void getPrintKGroupedStream(KGroupedStream<String, EnrichedWithAll> groupByProductId_on_EnrichedWithAll) {


        // Note: this will not print all the KGroupedStream records, which we might be expecting
        // instead it will print only the latest record of each Product/GameID as using reduce
        System.out.println("\n-----Printing KGroupedStream-----"); // this will not be pritned in stream
        // Perform the reduce operation to aggregate the records by key
//        KTable<String, EnrichedWithAll> reducedTable = groupByProductId_on_EnrichedWithAll.reduce(
//                (aggValue, newValue) -> newValue,
//                Materialized.with(Serdes.String(), JsonSerdes.EnrichedWithAll()) // creating a state-store using materialized framework
//                                // will create a new temporary topic named 'xyz-changelog'
//        );

        // Note: By default, KTables store only the latest value for each key,
        // so you won't be able to retrieve all historical values for a key directly from the KTable.
        KTable<String, EnrichedWithAll> tableFromStream = groupByProductId_on_EnrichedWithAll.aggregate(
                // Initializer - this initializes the aggregation state
                () -> null, // You can use a more appropriate initializer if needed

                // Aggregator - this function accumulates values for the same key
                (key, value, aggregate) -> value,

                // Materialized - specify how the result should be materialized, including Serdes
                // Means now the groupby setting will be stored in an custom defined State-Store
                // Note, KTable only store the latest state of any record/event
                // So, we are not expecting it to give me the a whole list of records, instead the latest record of each Product/GameID
                Materialized.<String, EnrichedWithAll, KeyValueStore<Bytes, byte[]>>
                                as("kgrouped-stream-intemidiary-state-store")

                        // for serialization and deserialization of key (product/GameID) and value (HighScores JavaObject)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerdes.EnrichedWithAll())
        );


        // Print the contents of the KTable - ** NOT HELPFUL, only get the latest score, not all scores of each Product/GameID
//        reducedTable.toStream()
//                .foreach((key, value) -> {
//                    System.out.println("Key: " + key);
//                    System.out.println("Value: " + value);
//                });
        // Write the results to the 'kgroup-interm' topic
        //reducedTable.toStream().to("kgroup-interm"); // no need to publish to a new stream

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


