package com.example.restful_services;

import com.example.models.CombinedVitals;
import io.javalin.Javalin;
import io.javalin.http.Context;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/*
REST API Desing
http://host:port/api_name
 */
/*
Topics at hand after all the Operations at PatientMonitoringTopology
> kafka-topics --bootstrap-server localhost:9092 --list
__consumer_offsets
alerts
body-temp-events
dev-consumer-KSTREAM-JOINOTHER-0000000021-store-changelog
dev-consumer-KSTREAM-JOINTHIS-0000000020-store-changelog
dev-consumer-KSTREAM-MAP-0000000011-repartition
dev-consumer-KTABLE-SUPPRESS-STATE-STORE-0000000006-changelog
dev-consumer-ss-pulse-counts-changelog
pulse-events

 */
@Data
@AllArgsConstructor // cant use NoArgsConstructor as several attributes defined 'final'
public class PatientMonitoringService {
    // if variable defined 'final', they have to be initialized // we can avoid this with Lomboks annotation @Data
    private final HostInfo hostInfo; // Kafka Wrapper class; contains both 'host' and 'port' information
    private final KafkaStreams streams; // REST Api would need to access to the Kafka Stream State-stores or topics
                                        // This Stream is designed in PatientMonitoringTopology using StreamBuilder

    // To log the activities in the console // its a Slf4j logger, not JavaUtil logger
    private static final Logger log = LoggerFactory.getLogger(PatientMonitoringService.class);

    // -------------- A. Expose 1x state-store and 1x SINK topic to external world --------------
    // A1. Expose state-store "ss-pulse-counts" to external world
    // Create a Read-only State-store of our materialzied 'ss-pulse-counts' outside of Kafka topology
    // Event/Record in the materialized 'ss-pulse-counts' state-store is
    // Windowed_multidimensional_KEY_[1_PatientID@1606122120000_lowerBoudnary/1606122180000_upperBoundary], Value_120_pulseCounts

    // ReadOnleWindowStore<K,V>
    // Here, Key-> STRING_1_PatientID@1606122120000_lowerBoudnary/1606122180000_upperBoundary | key is an WindowedMultidimensional key
    // Value -> 120_pulseCounts
    public ReadOnlyWindowStore<String,Long> getBpmStore(){
        return  streams.store(
                StoreQueryParameters.fromNameAndType(
                        "ss-pulse-counts",
                        QueryableStoreTypes.windowStore()
                )
        );
    }

    // A2. Expose SINK Topic 'alerts' to external world
    // Records in TOPIC 'alerts' is key_valued // Not windowed
    // Record/Example -> 1_patientID_Key_String, Value_CombinedVitals(heartRate=120_Long, bodyTemp_Object=BodyTemp(timestamp=2020-11-23T09:03:06.500Z, temperature=101.2, unit=F))
    public ReadOnlyKeyValueStore<String, CombinedVitals> getAlertsStore() {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        "alerts",
                        QueryableStoreTypes.keyValueStore()
                )
        );
    }

    // ------------ B. Start RESTFull server through Javalin ---------------
    // Which Server and Client components to be used?
    // Server component: Javalin to implement a REST service
    // Client Component: OkHttp to implement our REST Client

    public void start(){
        // Create REST Server to handle all REST API requests
        Javalin app = Javalin.create().start(hostInfo.port());

        // REST Client through OkHttp3
        // local window store query: all entries
        // GET http://localhost:8080/bpm/all
        app.get("/bpm/all", this::getAllWindowedBpmCountForAllPatients_for_all_timeframe);

        // GET http://localhost:8080/bpm/:from/:to
        // GET http://localhost:8080/bpm/2020-11-12T09:02:00.00Z/2020-11-12T09:03:00Z
        app.get("/bpm/range/:from/:to", this::getAllWindowedBpmForAllPatients_filter_by_timeframe);

    }

    // 1. GET http://localhost:8080/bpm/all
    // get all BPM entries from ReadOnly Windowed State Store exposed outside the Kafka Processor Topology using getBpmStore()
    // Here, Context -> is a Javalin Context which will show us the result in JSON
    // Records@BpmStore: Windowed_multidimensional_KEY_[1_PatientID@1606122120000_lowerBoudnary/1606122180000_upperBoundary], Value_120_pulseCounts
    /*
    Expected Output:

    {
      "[1@1606122180000/1606122240000]": 7,
      "[1@1606122120000/1606122180000]": 120
    }
     */
    public void getAllWindowedBpmCountForAllPatients_for_all_timeframe(Context ctx){
        // Define a HahMap as a placeholder of BPM <Key,Value> records
        Map<String, Long> bpmPlaceholder = new HashMap<>();

        // get the KeyValue Iterator for all the BpmStore events/records
        KeyValueIterator<Windowed<String>, Long> range = getBpmStore().all();

        // iterate over the BPM store records
        while (range.hasNext()){
            // fetch the first entry from iterator
            KeyValue<Windowed<String>, Long> nextRecord = range.next();
            // Get the key
            Windowed<String> key = nextRecord.key;
            // get the value
            Long value = nextRecord.value;

            // put both (key,value) into a HashMap
            bpmPlaceholder.put(key.toString(), value);

            log.info("Record for Patient/@Timerange {} having BPM count/min {}",key.toString(),value);
        }

        // close the iterator to avoid memory leaks
        range.close();

        // return the context data as JSON in the WebBrowser to OkHttp/Client component
        ctx.json(bpmPlaceholder);
    }

    // 2. GET http://localhost:8080/bpm/range/:from/:to
    // Example: http://localhost:8080/bpm/range/1606122120000/1606122180000
    // Windowed_multidimensional_KEY_[1_PatientID@1606122120000_lowerBoudnary/1606122180000_upperBoundary], Value_120_pulseCounts
    // Expected Output
    /*
    [
      {
        "start": "2020-11-23T09:02:00Z",
        "end": "2020-11-23T09:03:00Z",
        "value": 120,
        "key": "1"
      },
      {
        "start": "2020-11-23T09:03:00Z",
        "end": "2020-11-23T09:04:00Z",
        "value": 7,
        "key": "1"
      }
    ]
     */
    void getAllWindowedBpmForAllPatients_filter_by_timeframe(Context ctx){
        // Define an ArrayList to HashMap placeholder of <1_Paitent_id, Object{fromTimeMillis, toTimeMillis, pulseCount}>
        List<Map<String, Object>> bpmList = new ArrayList<>();

        // fetch the range from path parameter 'from' and 'to'
        // from -> 1606122120000 // unix epoch format // number of miliseconds have passed since Janunary 1, 1970 at 00:00:00:00 UTC
        // to -> 1606122180000
        String from = ctx.pathParam("from");
        String to = ctx.pathParam("to");

        // Convert the from and to to milliseconds epochs
        Instant fromTime = Instant.ofEpochMilli(Long.valueOf(from)); // convert miliseconds time to year-month format
                                                                    // 2020-11-12T09:02:00.00Z
        Instant toTime = Instant.ofEpochMilli(Long.valueOf(to)); // 2020-11-12T09:03:00Z

        // Define the KeyValue iterator to fetch all the Records/events from the BpmStore within this time range
        KeyValueIterator<Windowed<String>,Long> rangeIterator = getBpmStore().fetchAll(fromTime,toTime);

        // iterate over the range
        while (rangeIterator.hasNext()){
            // get the first record and the nexts
            KeyValue<Windowed<String>,Long> nextRecord = rangeIterator.next();
            // the record looks like

            // fetch the key and subkeys
            // Key_[1_PatientID@1606122120000_lowerBoudnary/1606122180000_upperBoundary], Value_120_pulseCounts
            String keyPatientId = nextRecord.key.key(); // 1_PatientID
            Window window = nextRecord.key.window(); // 1606122120000_lowerBoudnary/1606122180000_upperBoundary
            Long start = window.start(); // 1606122120000
            Long end = window.end(); // 1606122180000

            // fetch the value //Pulse count
            Long valueBpmCount = nextRecord.value; //Pulse count

            // crate a HashMap placeholder to hold all these fetched info
            Map<String,Object> singleRecordBpmPlaceholder = new HashMap<>();
            // place all these key, subkeys and value into a HashMap placeholder
            singleRecordBpmPlaceholder.put("patientId",keyPatientId);
            singleRecordBpmPlaceholder.put("start",Instant.ofEpochMilli(start).toString()); // convert milisecinds time to year-date format// 2020-11-12T09:02:00.00Z
            singleRecordBpmPlaceholder.put("end",Instant.ofEpochMilli(end).toString()); //2020-11-12T09:03:00Z
            singleRecordBpmPlaceholder.put("bpm",valueBpmCount);
            bpmList.add(singleRecordBpmPlaceholder);

        }

        // close the rangeIterator to avoid memory leaks
        rangeIterator.close();

        // return a JSON representation
        ctx.json(bpmList);


    }



}
