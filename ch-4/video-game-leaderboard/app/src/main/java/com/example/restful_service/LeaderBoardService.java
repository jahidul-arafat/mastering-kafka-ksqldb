package com.example.restful_service;

import com.example.model.restful_exposed_models.HighScores;
import com.example.model.stateful_join_models.EnrichedWithAll;
import io.javalin.Javalin;
import io.javalin.http.Context;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor // dont use NoArgsConstructor as the Instance Attributes are defined to be 'final' and must be initialized
public class LeaderBoardService {
    // Instance Attributes
    private final HostInfo hostInfo; // Kafka Stream Wrapper class; Containing hostname and port
    private final KafkaStreams streams; // to keep track of local kafka stream instance
                                        // this will coordinate with other kafka stream instance with the same applicatio id
                                        // Developed using DAG - Directed Acyclic Graph (DAG) or StreamBuilder
                                        // In our case, we used StreamBuilder To build Kafka topology

    private static final Logger log = LoggerFactory.getLogger(LeaderBoardService.class);


    // Method-1:
    // Create a Read-only State-Store outside of Kafka Processor Topology to expose to external world
        /*
        But Why we to expose this Read-Only State-Store to external world?
        - to develop an event-driven microservice with extremely low latency
         */
    // Put this in a separate class named "LeaderboardService""
    // Create a queryable read-only copy of the state-store "top3-high-scores-per-game-state-store"
    // ReadOnlyKeyValueStore<K,V> is an interface
    // Records/Event in the original state-store
        /*
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
    QueryableStateStore types
    - keyValueStore() -> Available methods are:
            - get(key)  -> Returns Value
            - range (key_from, key_to)  -> returns KeyValueIterator<K,V>
            - all() -> returns KeyValueIterator<K,V>
            - approximateNumEntries() -> returns Long
     */

    public ReadOnlyKeyValueStore<String,HighScores> getReadOnlyStateStore(){
        return streams.store(StoreQueryParameters.fromNameAndType(
                "top3-high-scores-per-game-state-store", // exisitng state-store name
                QueryableStoreTypes.keyValueStore()     // state-store type // multiple state-store types available
        ));
    }

    public void start() {
        Javalin app = Javalin.create().start(hostInfo.port()); // REST Server using Javalin

        /** Local key-value store query: point-lookup / single-key lookup */
        // mapping the URL path /leaderboard/:key to a GET request method getKey()
        // to show the high-score of a given Poduct/GameID_Key
        // We will use Point lookup to retrieve a single value from our read-only state-store
        // http://localhost:8080/leaderboard/6
        app.get("/leaderboard/:key", this::getKey); // :key -> means its an URL parameter i.e. 1

        /** Local key-value store query: approximate number of entries */
        // http://localhost:8080/leaderboard/count/all
        app.get("/leaderboard/count/all", this::getCountAllFromLocalRemote);

        /** Local key-value store query: approximate number of entries */
        // http://localhost:8080/leaderboard/count/local
        app.get("/leaderboard/count/local", this::getCountOnlyLocal);

        /** Local key-value store query: all entries */
        // http://localhost:8080/leaderboard
        app.get("/leaderboard", this::getAll);

        /** Local key-value store query: range scan (inclusive) */
        // http://localhost:8080/leaderboard/1/10
        app.get("/leaderboard/:from/:to", this::getRange); // Java method reference
                                                    // reference to the 'getRange' mthod of the current object of 'LeaderBoardService' class


    }

    // Implementing .get() method of ReadOnlyKeyValueStore
    /*
    Scenario to Consider
    - 2x Running instances of our Kafka Stream Application 'com.example.App'
        - Instance-01(local)/ at localhost:8080, Having Keys: {1,2,3}    -> I am here, execute a query to lookup for key {5}
        - Instance-02(remote)/ at localhost:8081, Having Keys: {4,5,6} // means our state i.e. 'top3-high-scores-per-game-state-store' is partitioned across multiple application instances
        - If no communication between instance-1 localhost:8080 and instance-2 localhost:8081, then we will fail to retrieve the key {5}
    - How to solve this problem?
        - Using 'queryMetadataForKey'
        - to discover both Instance-01(local)/ and Instance-02(remote)/ that a specific key lives on

    - Which instance we query and when we issue the query
        - State can move around whenever there is a consumer rebalance
        - if so, then we may not be able to retrive the requested value
     */
    public void getKey(Context ctx) { // Context - Javaline Context which will show us the result as JSON
        // Use a point lookup to retrieve a single value from our read-only state-store
        String productId = ctx.pathParam("key"); // key- Product/GameId - 1, 6

        // Improving the implementation
        // Solving the remote communication issues if state is distributed across multiple application instances
        // I.e. we look for Key {5} of application 'com.example.App' which is in the App Instance-02/remote, where
        // we are triggering the key search at Instance-01(local)
        // Step-1: Find which host/application instance a specific key should live on i.e. @instance-2

        // Consumer rebalancing (if multiple Kafka Stream Application Instance) operation at Kafka Stream
        // may make a Key unavailable for a while
        /*
        - if my Kafka
         */
        /*
        If "top3-high-scores-per-game-state-store" is a distributed state-store where
        state is stored in multiple partitions for multiple application instances, then
        we need a strategy to join these distributed state-stores to look for a specific key

        And the very first step would be 'to find all the available application instances operating on a specific state-store'
         */
        KeyQueryMetadata metadata = streams
                .queryMetadataForKey(
                        "top3-high-scores-per-game-state-store", // Instance-1: Storing Key: {1,2,3} at Partition-01
                                                                            // Instance-2: Storing Key: {5,6,7} at Partition-02
                        productId, // Key to search i.e. 5
                        Serdes.String().serializer() // Covert the "5" String to byte; as Kafka is a Byte-in and Byte-out stream
                        );

        // Step-1.1 I.e. Key {1_Key_looking_for} found in the local instance-01
        // First check in the local instance-01 if the Key {1} is found or not
        if (hostInfo.equals(metadata.activeHost())){
            log.info("Querying Key {} in the local store", productId);
            HighScores highScores = getReadOnlyStateStore().get(productId); // local query // query the local store

            // i.e. the Product/GameId {1} not found
            if (highScores == null){
                ctx.status(404);
                log.info("Key {} not found in the local store", productId);
                return;
            }

            // i.e. the Product/GameId {1} found
            // get the list of values from the Key and convert this to json for the Javalin Context
            ctx.json(highScores.toList());
            log.info("Key {} found in the local store", productId);
            return;
        }

        // Step-1.2 Host is not the localhost, instead a remote host i.e. the remote instance
        // Remote instance has the key {5}
        queryFetchKeyFromRemoteHost(ctx, metadata, productId);
    }

    private void queryFetchKeyFromRemoteHost(Context ctx, KeyQueryMetadata metadata, String productId) {
        String remoteHost = metadata.activeHost().host(); // remote host i.e. Instance-02
        int remotePort = metadata.activeHost().port(); // remote port i.e. 8081
        String url = String.format(
                "http://%s:%d/leaderboard/%s",
                remoteHost, remotePort, productId);
        // Browse the remote instance-2 URL with OkHttpClient to make a URL request
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder().url(url).get().build(); // build the request

        // invoke the request and get the response body else HTTP 500 error
        try (Response response = client.newCall(request).execute()) {
            log.info("Querying Key {} in the remote store", productId);
            log.info("Key {} found the remote store", productId);
            ctx.result(response.body().string()); //
        } catch (IOException e) {
            log.info("Key {} not exits in both local and remote store", productId);
            ctx.status(500); // HTTP 500 - Internal Server Error
        }
    }


    /** Local key-value store query: approximate number of entries */
    // How Designed: app.get("/leaderboard/count/local", this::getCountLocal);
    // Get the count from both the local-instance and the remote-instance
    public void getCountAllFromLocalRemote(Context ctx) { // Context - Javaline Context which will show us the result as
        // get the record counts from the local-instance
        // current one
        long count = getReadOnlyStateStore().approximateNumEntries(); //get approximate number of entries of the read-only state-store
                                                                        // this is RockDB store, not in-memory store
                                                                        // Count will not be accurate

        // foreach
        // metadata of all the hosts i.e. localhost and remotehost having the state store 'top3-high-scores-per-game-state-store'
        for (StreamsMetadata metadata:
                streams.allMetadataForStore("top3-high-scores-per-game-state-store")) {

            // as localhost records are already in the count, escape those to recount
            // to bypass local instance to avoid recounting and only count the records from remote-instance
            if (!hostInfo.equals(metadata.hostInfo())){ // !localhost.equals(localhost)= !True = False
                log.info("bypassing local store count");
                continue;
            }
            // get the count from the remote-instance
            count+= fetchCountFromRemoteInstance(metadata.hostInfo().host(), metadata.hostInfo().port());
        }

        log.info("(Local + Remote) Approximate number of entries: {}", count);
        ctx.json(count);
    }

    public void getCountOnlyLocal(Context ctx) { // Context - Javaline Context which will show us the result as
        log.info("Getting count from the Local store only");
        long count = 0L;
        try {
            count = getReadOnlyStateStore().approximateNumEntries(); //get approximate number of entries of the read-only state-store
            // this is RockDB, not in-memory store
            // RocksDB having duplicate keys, thereby count will not be accurate
        } catch (Exception e) {
            // log error
            log.error("Could not get leaderboard count", e);
        } finally { // finally block will always execute
            //ctx.result(String.valueOf(count));
            ctx.json(count);
            log.info("(local) Approximate number of entries: {}", count);
        }
    }

    // For getCountAllLocalRemote()
    // Build the remote call request URL to fetch the records in remote-instance
    private long fetchCountFromRemoteInstance(String host, int port) {
        log.info("Getting count from the remote store");
        OkHttpClient client = new OkHttpClient(); // creating the REST Client using OkHttpClient

        // Building the remote instance call URL along with a RequestBuilder
        String url = String.format("http://%s:%d/leaderboard/count/local", host, port); //this is not local; its the local of remote
        Request request = new Request.Builder().url(url).build();

        try (Response response = client.newCall(request).execute()) {
            return Long.parseLong(response.body().string());
        } catch (Exception e) {
            // log error
            log.error("Could not get leaderboard count", e);
            return 0L;
        }
    }

    // method to get all the records
    /*
    i.e. in From the ReadOnlyStateStore of 'top3-high-scores-per-game-state-store' we get
    1(GameID)-> [Score_100, Score_50, Score_20]
    2(GameID)-> [Score_200, Score_150, Score_70]

    Expected result/ A List:
    [Score_100, Score_50, Score_20, Score_200, Score_150, Score_70] // order is not guaranteed
     */
    public void getAll(Context ctx) { // Context - Javaline/JVM Context which will show us the result as
        log.info("[getAll] Getting all records from the ReadOnlyKeyStore 'top3-high-scores-per-game-state-store'");

        // Define a HashMap as a placeholder for KeyValueIterator 'range'.all
        Map<String,List<EnrichedWithAll>> leaderboardMap = new HashMap<>();

        // Returns:
        //An iterator of all key/value pairs in the store, from smallest to largest bytes.
        // Order is not guaranteed
        // HighScores are storing the EnrichedWithAll object in a TreeSet
        // i.e. [From the HighScores class] private final TreeSet<EnrichedWithAll> highScoreSet = new TreeSet<>();
        KeyValueIterator<String,HighScores> rangeIteratorFromReadOnlyKeyValueStore = getReadOnlyStateStore().all();
        int keyCounter=0;
        int valueCounter=0;

        // iterate over each record
        // each record has a Key and a List of Values
        while (rangeIteratorFromReadOnlyKeyValueStore.hasNext()) {
            var nextRecord = rangeIteratorFromReadOnlyKeyValueStore.next(); // First point to Key_GameID_1, then next to Key_GameID_2
            String gameId = nextRecord.key;
            HighScores top3ScoreRecordTreeSet = nextRecord.value; // these are EnrichedWithAll values in the HighScores object
            List<EnrichedWithAll> top3ScoreRecordList = top3ScoreRecordTreeSet.toList(); //
            leaderboardMap.put(gameId, top3ScoreRecordList);
            log.info("[getAll] Fetching the EnrichedWithAll {} records for GameID {}", top3ScoreRecordList.size(),gameId);

            keyCounter++;
            valueCounter+=top3ScoreRecordList.size();
        }
        rangeIteratorFromReadOnlyKeyValueStore.close(); // must have to close to avoid memory leak
        log.info("[getAll] Closing the rangeIterator to avoid memory leak. " +
                "Total Read-> Key {}, Value {}",keyCounter,valueCounter);

        // covert the map into JSON for the Javaline/JVM Context
        ctx.json(leaderboardMap);

    }

    /** Local key-value store query: range scan (inclusive) */
    //app.get("/leaderboard/:from/:to", this::getRange);
    // http://localhost:8080/leaderboard/1/6 - inclusive range
    public void getRange(Context ctx) { // Context - Javaline/JVM Context which will show us
        log.info("[getRange] Getting all records from the ReadOnlyKeyStore 'top3-high-scores-per-game-state-store'");
        // from the URL path, get the 'from' and 'to' pathParameters
        String from = ctx.pathParam("from");
        String to = ctx.pathParam("to"); // inclusive

        // Get the Iterator data from the ReadOnlyKeyStore 'top3-high-scores-per-game-state-store'
        // String_Key_1: Value_HighScoresObject
        KeyValueIterator<String,HighScores> rangeIteratorFromReadOnlyKeyValueStore =
                getReadOnlyStateStore().range(from, to);
        int keyCounter=0;
        int valueCounter=0;

        // define a HashMap as a placeholder for the Iterator data
        // Note- the iterator value 'HighScores' has toList() method to get all the EnrichedWithAll objects
        Map<String,List<EnrichedWithAll>> leaderboardMap = new HashMap<>();

        // loop through the Iterator
        while (rangeIteratorFromReadOnlyKeyValueStore.hasNext()) {
            var nextRecord = rangeIteratorFromReadOnlyKeyValueStore.next(); // First point to Key_GameID_1, then next to Key_GameID_2
            String gameId = nextRecord.key;
            HighScores top3ScoreRecordTreeSet = nextRecord.value; // these are EnrichedWithAll values in the HighScores object
            List<EnrichedWithAll> top3ScoreRecordList = top3ScoreRecordTreeSet.toList(); //
            leaderboardMap.put(gameId, top3ScoreRecordList);
            log.info("[getRange] Fetching the EnrichedWithAll {} records for GameID {}", top3ScoreRecordList.size(),gameId);
        }

        rangeIteratorFromReadOnlyKeyValueStore.close(); // must have to close to avoid memory leak
        log.info("[getRange] Closing the rangeIterator to avoid memory leak. " +
                "Total Read-> Key {}, Value {}",keyCounter,valueCounter);

        // covert the map into JSON for the Javaline/JVM Context
        ctx.json(leaderboardMap);
    }


}
