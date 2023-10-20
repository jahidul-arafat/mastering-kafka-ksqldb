package com.example.restful_services;

import com.example.models.DigitalTwin;
import io.javalin.Javalin;
import io.javalin.http.Context;
import lombok.AllArgsConstructor;
import lombok.Data;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Data
@AllArgsConstructor // cant use NoArgsConstructor as several attributes defined 'final'
public class DigitalTwinRestService {
    // if variable defined 'final', they have to be initialized // we can avoid this with Lomboks annotation @Data
    private final HostInfo hostInfo; // Kafka Wrapper class; contains both 'host' and 'port' information
    private final KafkaStreams streams; // REST Api would need to access to the Kafka Stream State-stores or topics
    // This Stream is designed in PatientMonitoringTopology using StreamBuilder

    // To log the activities in the console // its a Slf4j logger, not JavaUtil logger
    private static final Logger log = LoggerFactory.getLogger(DigitalTwinRestService.class);

    // -------------- A. Expose 1x state-store to external world --------------
    // A1. Expose state-store "digital-twin-store" to external world
    // Create a Read-only State-store of our materialzied 'digital-twin-store' outside of Kafka topology
    // "digital-twin-store" state-store is a key-value store, not an windowed store
    public ReadOnlyKeyValueStore<String, DigitalTwin> getDigitalTwinReadOnlyStore(){
        return  streams.store(
                StoreQueryParameters.fromNameAndType(
                        "digital-twin-store",
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }

    // ------------ B. Start RESTFull server through Javalin ---------------
    // Which Server and Client components to be used?
    // Server component: Javalin to implement a REST service
    // Client Component: OkHttp to implement our REST Client

    public void start() {
        // Create REST Server to handle all REST API requests
        Javalin app = Javalin.create().start(hostInfo.port());

        // REST Client through OkHttp3
        // get Turbine state by Turbine ID
        // GET http://localhost:8080/devices/1
        app.get("/devices/:id", this::getDevice); //:id -> means its an URL parameter i.e. 1
    }

    // 1. GET Turbine/Device State
    // http://localhost:8080/devices/1
    public void getDevice(Context ctx) { // Context - Javaline Context which will show us the result as JSON
        // Use a point lookup to retrieve a single value from our read-only state-store
        String deviceId = ctx.pathParam("id"); // id- Turbine/DeviceId - 1, 6

        // Improving the implementation
        // Solving the remote communication issues if state is distributed across multiple application instances
        // I.e. we look for Key/Device {5} of application 'com.example.apps.ProcessorApp' which is in the App Instance-02/remote, where
        // we are triggering the key search at Instance-01(local)
        // Step-1: Find which host/application instance a specific key should live on i.e. @instance-2

        // Consumer rebalancing (if multiple Kafka Stream Application Instance) operation at Kafka Stream
        // may make a Key unavailable for a while
        /*
        If "digital-twin-store" is a distributed state-store where
        state is stored in multiple partitions for multiple application instances, then
        we need a strategy to join these distributed state-stores to look for a specific key

        And the very first step would be 'to find all the available application instances operating on a specific state-store'
         */
        KeyQueryMetadata metadata = streams
                .queryMetadataForKey(
                        "digital-twin-store", // Instance-1: Storing Key: {1,2,3} at Partition-01
                        // Instance-2: Storing Key: {5,6,7} at Partition-02
                        deviceId, // Key to search i.e. 5
                        Serdes.String().serializer() // Covert the "5" String to byte; as Kafka is a Byte-in and Byte-out stream
                );

        // local instance has this key
        // Step-1.1 I.e. Key {1_Key_looking_for} found in the local instance-01
        // First check in the local instance-01 if the Key {1} is found or not
        if (hostInfo.equals(metadata.activeHost())){
            log.info("Querying Key {} in the local store", deviceId);
            DigitalTwin latestState = getDigitalTwinReadOnlyStore().get(deviceId); // local query // query the local store
                                                                                    // this get() method is the method defined in the ReadOnlyKeyValue interface
            // i.e. the Device/TurbineId {1} not found
            if (latestState == null){ // no record/value found for the i.e key_1
                // digitalTwin record not found in the local store
                ctx.status(404); // HTTP Not Found error
                log.info("TurbineId/Key {} not found in the local store", deviceId);
                return;
            }

            // i.e. the Device/TurbineId {1} found
            // convert this to json for the Javalin Context
            ctx.json(latestState);
            log.info("TurbineId/Key {} found in the local store", deviceId);
            return;
        }

        // Step-1.2 Host is not the localhost, instead a remote host i.e. the remote instance
        // Remote instance has the key {5}
        queryFetchKeyFromRemoteHost(ctx, metadata, deviceId);
    }

    private void queryFetchKeyFromRemoteHost(Context ctx, KeyQueryMetadata metadata, String deviceId) {
        String remoteHost = metadata.activeHost().host(); // remote host i.e. Instance-02
        int remotePort = metadata.activeHost().port(); // remote port i.e. 8081
        String url = String.format(
                "http://%s:%d/devices/%s", // http://localhost:8081/devices/1 // OkHttp3 will hit this URL to check for key TurbineId in the "digital-twin-store" distributed state-store distributed across both local and remote host
                remoteHost, remotePort, deviceId);

        // Browse the remote instance-2 URL with OkHttpClient to make a URL request
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder().url(url).get().build(); // build the request

        // invoke the request and get the response body else HTTP 500 error
        try (Response response = client.newCall(request).execute()) {
            log.info("Querying Key {} in the remote store", deviceId);
            log.info("Key {} found the remote store", deviceId);
            ctx.result(response.body().string()); //
        } catch (IOException e) {
            log.info("Key {} not exits in both local and remote store", deviceId);
            ctx.status(500); // HTTP 500 - Internal Server Error
        }
    }

}
