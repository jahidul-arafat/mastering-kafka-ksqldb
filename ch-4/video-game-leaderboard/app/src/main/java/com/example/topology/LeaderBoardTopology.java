package com.example.topology;

import com.example.model.Player;
import com.example.model.Product;
import com.example.model.ScoreEvent;
import com.example.model.stateful_join_models.EnrichedWithAll;
import com.example.model.stateful_join_models.ScoredWithPlayer;
import com.example.serdes.wrapper.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;


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
        // Without Re-keying: Not suitable for join operation witn KTable 'players'
//        KStream<byte[], ScoreEvent> scoreEventSource = topologyStreamBuilder
//                .stream(
//                        "score-events",
//                        Consumed.with( // define how records will be consumed
//                                Serdes.ByteArray(), // Key of the record will be deserialized as ByteArray
//                                JsonSerdes.ScoreEvent() // Value of the record will be deserialized to ScoreEvent Object using JsonSerdes
//                        ));
//        getPrintStream(scoreEventSource, "score-events"); // STREAM PROCESSOR to print only | (Test Only)

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
                // rekeying require a temporary topic i.e. 'temp-score-events' generated automatcally, which is later read by the main Kafka Topic 'score-events'
                // So, Network trip is required, making rekey an expensive operation
                .selectKey((k, v) -> v.getPlayerId().toString()); // select the key for rekeying // rekeying using player_id

        getPrintStream(scoreEventSource, "score-events"); // STREAM PROCESSOR to print only | (Test Only)

        // B2. KTable Abstraction to read from topic "players" // partitioned across all application instances // time sync
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
                                Serdes.String(),
                                JsonSerdes.Product()
                        )
                );

        // Use the 'productSource' as needed here



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
        ValueJoiner<ScoreEvent,Player, ScoredWithPlayer> predicate_Score_Player_Joiner=
                (score,player)-> new ScoredWithPlayer(score,player); // lambda expression // but Scored

        // C1.2 Define Join Settings
        // Define the Serialization/Deserlialization settings for the keys and values when performing a stream-table join operation
        // Joined<Key, LeftTable, RightTable>
        Joined<String, ScoreEvent,Player> settings_playerJoinParams =
                Joined.with(
                        Serdes.String(), // indicates the keys in both tables are expected to be deserialized as String
                        JsonSerdes.ScoreEvent(), // how to deserialize the ScoreEvent object from binary data (JSON)
                        JsonSerdes.Player()); // how to deserialize the event/json data to Player object

        // C1.3 Perform Actual inner join operation; key at both table should match
        // KStream<Key,Value>
        // This join triggers error in Mac M1
        // rocksDB does not support Apple Silicon natively yet.
        KStream<String, ScoredWithPlayer> scoreWithPlayer =
                scoreEventSource.join(
                        playerSource,
                        predicate_Score_Player_Joiner,
                        settings_playerJoinParams);
        getPrintStream(scoreWithPlayer, "score-with-players");



        // C2. KStream-GlobalKTable Join: Joining 'ScoredWithPlayer' and 'Product/Game'
//        ValueJoiner<ScoredWithPlayer, Product, EnrichedWithAll> predicate_ScoreWithPlayer_Product_Joiner=
//                EnrichedWithAll::new; // lambda expression // but EnrichedWithAll constructor implemented in a different way




        // Return the topology to App.main()
        Topology topology = topologyStreamBuilder.build();
        return topology;
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


