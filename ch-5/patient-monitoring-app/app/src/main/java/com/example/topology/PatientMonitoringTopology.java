package com.example.topology;

import com.example.models.BodyTemp;
import com.example.models.CombinedVitals;
import com.example.models.Pulse;
import com.example.models.VitalTimestampExtractor;
import com.example.serdes.wrapper.JsonSerdes;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Duration;

public class PatientMonitoringTopology {
    private static final Logger log = LoggerFactory.getLogger(PatientMonitoringTopology.class);
    // Class Method-01
    public static Topology build() {
        // A. StreamBuilder to construct the Kafka Topology
        StreamsBuilder topologyStreamBuilder = new StreamsBuilder();

        // B. ----------------------------- Registering SOURCE Streams---------------------------------------

        // Adding 3X SOURCE PROCESSORS:: for 3X topics {pulse-events, body-temp-events, alert-sink}
        // B1. Source Processor for 'Pulse Events'
        // B1.1 KStream - unbounded stream; keyed,
        // Strategy: events/records have to be distributed in round-robin fashion in all 4x partitions of topic 'pulse-event'
        // (a) Define the Consumer Rules with the Custom TimeStampExtractor
        Consumed<String, Pulse> pulseConsumerOptions = Consumed.with(
                Serdes.String(),
                JsonSerdes.Pulse()
        ).withTimestampExtractor(new VitalTimestampExtractor());

        // (b) Register the events/records in a KStream, consumed by the Serdes rules defined
        // KStream<Key_PatientID_String, Value_event>
        KStream<String, Pulse> pulseEventsSource = topologyStreamBuilder
                .stream(
                        "pulse-events",
                        pulseConsumerOptions
                );
        getPrintStream(pulseEventsSource, "pulse-events");

        // B2. Source Processor for 'Body Temp Events'
        // B2.1 Using KStream<Key_PatientID_String, Value_event
        // Strategy: Let the body-temp events distibuted in round-robin fashion accross all 4x partitions of topic 'body-temp-events'
        // (a) Define the Consumer Rules with the Custom TimeStampExtractor
        Consumed<String, BodyTemp> bodyTempConsumerOptions = Consumed.with(
                Serdes.String(),
                JsonSerdes.BodyTemp()
        ).withTimestampExtractor(new VitalTimestampExtractor());

        // (b) Register the events/records in a KStream, consumed by the Serdes rules defined
        // KStream<Key_PatientID_String, Value_event>
        KStream<String, BodyTemp> bodyTempEventsSource = topologyStreamBuilder
                .stream(
                        "body-temp-events",
                        bodyTempConsumerOptions
                );
        getPrintStream(bodyTempEventsSource, "body-temp-events");
        //---------------- SOURCE STREAM REGISTERING (END) --------------------------------

        // C.-------------------- WINDOWING STREAMS ---------------------------------------------------
        /*
        Target: Only count the records that fall wintin each 60-seconds windows.
        Goal: Calculate the number of beats per minute(bpm) per patient
        Why Windows: To group records/events with "close temporal proximity". However, "close temporal proximity" varies
            - if event-time semantics: events/records that occured "around the same time"
            - if processing-time semantics: events/records that processed "around the same time"
            ** Here, window size = 1min, 5min etc
            ** Session windows

        ********************* 4x types of Kafka Windows ****************************
            (a) Tumbling windows:
                - fixed (predictable time range) sized windows (never overlap).
                - aligned with epochs (one complete pass through the entire tranining dataset
                (or mini-batch gradient descent) during the training phase of a model)

                TimeWindows tumblingWindow = TimeWindows.of(Duration.ofSeconds(5)); # a tumbling window of 5 seconds
                (1-5)(6-10)(11-15)

             (b) Hopping windows: fixed-sized windows that can overlap (window size + advance interval::how much the window move forward)
                - if Advanced Interval < WindowSzie -> windows overlap, means some records will appear in multiple windows
                - StartTime -> is inclusive, EndTime -> is exclusive

                (1-5)(4-9)(8-12)(11-15)
                TimeWindows hoppingWindow =  TimeWindows
                                            .of(Duration.ofSeconds(5))
                                            .advanceBy(Duration.ofSeconds(4)); // cause overlap
              (c) Session windows: variable-sized windows (period of activity followed by gaps of inactivity)
                - required parameter: inactivity gap

                SessionWindows sessionWindow = SessionWindows.with(Duration.ofSeconds(5));

                - Say, new records appears within 5s of previous record -> merged into the same window
                    - hot keys leading to long window ranges
                - Say, new records appears after 5s of previous record -> a new window will be created
                    - less active keys leading to shorter windows

              (d) Sliding join windows

              JoinWindows joinWindow = JoinWindows.of(Duration.ofSeconds(5));
              // Timestamps must be less than or equal to five seconds apart to fall within the same window.

                 - Say, Record1 (key1:value_1:@10.01) , Record2 (key2:value_2:@10.03)
                        - Timestamp difference <=5seconds.
                        - Both Record1 and Record2 merged into the same window
                 - Say, Record3 (key1:value_1:@10.05) , Record4 (key2:value_2:@10.13)
                        - Timestamp difference >5seconds.
                        - No join operation between Record1 and Record2

              (e) Sliding aggregation windows (Since Kafka 2.7.0)- No epochs
              SlidingWindows slidingWindow =
                     SlidingWindows.withTimeDifferenceAndGrace(
                       Duration.ofSeconds(5),
                       Duration.ofSeconds(0));

         ****************** WHich Window is Good Fit for converting raw pulse events into a heart rate ***************
         - Note: Our window size is fixed: 60s (non-overlapping); will be using epochs
         - XXXX Session Window - NOT A GOOD FIT; indifinte as long as there is activity on the stream
         - XXXX Sliding windows
         - XXXXX Sliding Aggregation windows - we will be using epochs
         - XXXXX hopping windows - No overlapping
         - Tumpling windows - RIGHT CHOICE

         ******* Decision: Tumbling windows
         */
        // C1.1 Calculate Heart rate (bpm) from pulse events (60s frame, fixed, non-overlapping, epochs)
        // Strategy-01: Create a Tumbling window of 60s // Default doesnt know how to handle delayed events/records
        //TimeWindows tumblingWindow = TimeWindows.of(Duration.ofSeconds(60)); // Class.method

        // Strategy-02: Create a Tumbling window of 60s // Must know how to deal with delayed events
        // introduce grace period of 5s lag for pulse events (we will use these timestamped pulse-events to calculate heart-rate bpm)
        TimeWindows tumblingWindow = TimeWindows
                .of(Duration.ofSeconds(60)) // windows size
                .grace(Duration.ofSeconds(5));  // if delayed events-> how long should wait?
        // Watch the trade-off between "Completeness Vs Latency"
        // High Grace Period -> optimize for completeness
        // But --> at the expense of higher latency (windows may left open for until the grace period is up, thereby no immediate result is triggered)


        // C1.2 Perform aggregation: group the pulse-event records by key "patientId"
         /*
        Why Grouping?
        - Same to rekeying
        - to make sure the related records are in the same partition processed by the same task observer

        How is Grouping done?
        - groupBy - similar to selectKey, as key changing operation, required repartitioning. R
                    - repartitining is costly, network call required
                    - requir when you have the event/records wiout key or you need to change the key of records
        - groupByKey - no repartition needed. Not costly as no repartition, thereby no network call required.
         */
        // ***** Decision: groupByKey() as 'pulse-events' records are already keyed and we dont need to rekey those
        // Not why the return type is KTable; because we are doing window aggregation

        // Syntax : [pulse-counts]: [<oldKey>@<window_start_ms>/<window_end_ms>], count
        // Expected output: [pulse-counts]: multidimensional_record_Key_[1@1606122180000/1606122240000], 7
        // Why Windowed<String> -> bcoz we are expecting a multi-domensional record key

        // Strategy-01:
        // GroupByKey();
        // single-key to multi-dimensional-key transformation:: converts KStream into KTable,
        // store the count in a state-store
        // Problem as shown below: Lots of unwanted intermediaities less than the desired 60s windows
//        KTable<Windowed<String>,Long> pulseCounts_aggregated =
//                pulseEventsSource // KStream
//                        .groupByKey()// doesnt change KStream to KTable
//                                    // prerequisit grouping: for aggregation:: count // avoid groupBy() to avoid unnecessary rekey
//                        .windowedBy(tumblingWindow) // Changed the KStream to KTable
//                                                    // window the pulse event in 60s timeframe
//                                                    // This converts the KTable<Key_String, Value_Long> to KTable<Windowed<String>, Value_long>
//                        .count(Materialized.as("ss-pulse-counts")); // store the aggregated result into a state-store named "pulse-count"


        // Strategy-02 : Suppress the results of the window so that only the final computation is emitted. No intermediate results as expected
        // Strategy01+
        // SUPPRESS strategy (
        //      get events::until_60s_window_close +
        //      unbound_Stream:: as keyspace is limited:: less number of patients +
        //      shutdown_application_if_buffer_full/ No early emit)

        // expected output: [pulse-counts]: [1_PatientID@1606122120000_lowerBoudnary/1606122180000_upperBoundary], 120_pulseCounts // means in 60s duration we got 120 pulse-events
        // discard any windos less than 60s
        KTable<Windowed<String>, Long> pulseCounts_aggregated =
                pulseEventsSource
                        .groupByKey()// groupby patientID
                        .windowedBy(tumblingWindow) // convert KStream into KTable // creates new multi-dimentional key: oldKey_1-> [<oldKey>@<window_start_ms>/<window_end_ms>]
                        .count(Materialized.as("ss-pulse-counts")) // store the aggregated result in state-store
                        .suppress( // suppressed to avoid intermediary emits as seen in earlier solution
                                Suppressed.untilWindowCloses(
                                        Suppressed.BufferConfig.unbounded().shutDownWhenFull()
                                )
                        );


        // C1.3 [Debug only] Convert the kTable into stream to print the contents into console
        var pulseCount_aggregated_stream = pulseCounts_aggregated.toStream();
        getPrintStream(pulseCount_aggregated_stream, "pulse-counts");


        // Problem Area:
        // **** Have you seen the peculiarity? Producing lots of intermediary as every event/record appears
        // how to solve the problem of intermediate results being emitted in our windowed heart rate aggregation
        /*
        [pulse-events]: 1, Pulse(timestamp=2020-11-23T09:03:00.000Z)            # event_1
        [pulse-counts]: [1@1606122180000/1606122240000], 1                      # intermediary 1  ----> why these intermediaries??
        [pulse-events]: 1, Pulse(timestamp=2020-11-23T09:03:00.500Z)            # event_2
        [pulse-counts]: [1@1606122180000/1606122240000], 2                      # intermediary_2
        [pulse-events]: 1, Pulse(timestamp=2020-11-23T09:03:01.500Z)            # event_3
        [pulse-counts]: [1@1606122180000/1606122240000], 3                      # intermediary_3
        [pulse-events]: 1, Pulse(timestamp=2020-11-23T09:03:02.500Z)            # event_4
        [pulse-counts]: [1@1606122180000/1606122240000], 4                      # intermediary_4
        [pulse-events]: 1, Pulse(timestamp=2020-11-23T09:03:03.500Z)            # event_5
        [pulse-counts]: [1@1606122180000/1606122240000], 5                      # intermediary_5
        [pulse-events]: 1, Pulse(timestamp=2020-11-23T09:03:04.500Z)            # event_6
        [pulse-counts]: [1@1606122180000/1606122240000], 6                      # intermediary_6
        [pulse-events]: 1, Pulse(timestamp=2020-11-23T09:03:05.500Z)            # event_7
        [pulse-counts]: [1@1606122180000/1606122240000], 7                      # final_count_7 // window closed
         */

        // ********* Analyzing why there are so many intermidiary i.e. why so many emitted windows result, instead of just the final one (we are interested)
        // Intermidiaries are unacceptable in our "patient monitoringg applications", bcoz we need to calculate a hear rate using 60s of data, not less than that
        // We cant calculate the heart rate less than 60 sencods of data. But intermediaries producing data for each seconds
        // We need to only emit the final result of a window
        /*
        This emit decision is complex due to two factors:
        (a) The unbound event stream may not always be in timestamp order
        (b) Events/records are sometimes delayed
        - These rises the question to tradeoff between "completeness vs latency"
        "Completeness: do we wait a certain amount of time for all of the data to arrive, or
        Latency: do we output the window result whenever it is updated"

        Solution Strategy:
        (a) To optimize Latency: Continuous Refinement
            - Kafka Stream will emit the new computation immediately (as seen in the intermediaries)
            - In continuous refinement, treat each event as incomplete; means events are not processed that fall into the windows timestamp

        (b) TO handle delayed data
            - Inspired by Apache Flink's Influenctial Dataflow model (leverage watermarks)
            - Watermarks defines when all the Data of a given window should have arrived
                - Factor1: window_size
                - Factor2: allowed_lateness of events -> configure it using "Grace_Period"
                    - Grace Period menas -> keeps the window open for a specif amount of time to admit delayed event

            Reconfigure the earlier tumblingWindow with Grace Period:
            TimeWindows tumblingWindow = TimeWindows
                .of(Duration.ofSeconds(60))
                .grace(Duration.ofSeconds(5)); // Watch the trade-off between "Completeness Vs Latency"

         ************ Solution: Use "suppress" operation to avoid intermediaries
         - These leads to 3x questions
            - Q1. Which Suppress strategy to use to get rid of intermediaries
            - Q2. How much memory should be used for buffering suppressed events (Buffer Config)
            - Q3. What to do when memory limit exceeded (Buffer Full Strategy)
         - 2x Suppress Strategy
            - (a) until window closes: only trigger final results from each windows (GOOD FOR OUR SOLUTION)
            - (b) until time limit exceeds: emit results from an windows if the specified time has elapsed (XXXXXXXX)
         - Ans/Q1/ Which one would we choose: "until window closes", as we need pulse event of a full 60s to calculate patient heartbeat
         - So, Kafka Stream will not emit results immediately, it will buffer
            - Ans/Q2/ Tell Kafka stream how to buffer unemitted events in memory :: use Buffer Configs
                (i)  Buffer Config_01: BufferConfig.maxBytes() (XXXXXXXXX)
                        -> how many suppressed events? constrained by number_of_bytes
                (ii) Buffer COnfig_02: BufferConfig.maxRecords() (XXXXXXXXXXX)
                        -> constrained by number_of_keys
                (iii) Buffer COnfig_03: BufferConfig.unbounded() -> no constraints; use as much heap space as needed to hold suppressed records
                                                                 -> Trigger: OOM (OutOfMemoryException)
                                                                 (GOOD FOR OUR SOLUTION) as our keyspace is limited
          - Ans/Q3/ What to do if buffer is full?
            (i) Strategy_01: Shutdown application if buffer full; (GOOD FOR OUR SOLUTION)
                        -> Drawback/ you will Never see any intermediary window computation
            (ii) Strategy_02: Emit oldest result if buffer is full/no application shutdown;
                        -> Adv/ You will see imtermidiaries

          - Finally, SUPPRESS chosen strategies to handle/avoid intermediaties of pulse-events to calculate Heartbeat BPM
            (a) Suppress Strategy: until window close, to get pulse_events of exact 60s window
            (b) Memory/Buffer strategy: BufferConfig.unbound(); as our keyspace is limited; though having the OOM execption risk
            (c) Handle BufferFull: Shutdown application if buffer is full;
                                   No early emit is desired as we need pulse-events of exactly 60s window. Less than that will not work in calculating HeartRate BPM
         */
        // Suppress is embedded into the code above

        // ------------------ D. Filtering and ReKeying --------------------------------
        // Windowed<String> has changed the old key to a new multi dimensional key
        // OldKey: PatientID_1
        // Output from our aggregated stream: [pulse-counts]: MultiDimensional_NewKey_[1@1606122120000/1606122180000], 120
        // NewKey Syntax: [<oldKey>@<window_start_ms>/<window_end_ms>]

        // D1. Filtering records exceeding threshold; means get only those records exceed the threshold
        // threshold: bodyTemp > 100.4F

        // D1. Rekey it to PatientID
        // Why Rekey? TO perform join with 'body-temp-events' which are Keyed by PatientID
        // Example events@'body-temp':1_PatientID|{"timestamp": "2020-11-23T09:03:06.500Z", "temperature": 101.2, "unit": "F"}

        // D1(filter)-> then-> D2(Rekey) // get the high pulses
        // From pulseCount_aggregated_stream we got: Key_[1@1606122180000/1606122240000], Value_120
        // Task-1: filter for any pulse that exceeds our threshold >= 100 pulse_count
        // Expected output: Key_1_patientID, Value_120
        KStream<String, Long> highPulse_filtered_rekeyed =
                pulseCount_aggregated_stream
                        // peek a record/event from the stream and print the log in the console
                        .peek(
                                (windowedKey, value)->{
                                    String patientID = new String(windowedKey.key());
                                    Long windowStart = windowedKey.window().start();
                                    Long windowEnd = windowedKey.window().end();
                                    log.info(
                                            "Patient {} has a heart rate of {} between {} and {}", // Patient 1 has a heart rate of 120
                                            patientID,value,windowStart,windowEnd);

                                }
                        )
                        .filter((windowedKey, value) -> value >= 100) // filter, if pulse count >=100; get all the high pulses count
                        .map((windowedKey, value) -> {   // rekey to Key_1_patientID again
                            // here windowedKey -> [1@1606122180000/1606122240000]
                            // windowedKey.key()-> 1
                            // windowedKey.value()-> 1606122180000/1606122240000
                            return KeyValue.pair(windowedKey.key(), value);
                        });
        // Task-1[Debug only] print the KStream
        // Expected output: [high-pulse-filtered-rekeyed]: 1, 120
        getPrintStream(highPulse_filtered_rekeyed, "high-pulse-filtered-rekeyed");

        // Task-2: filter any body-temp event that exceeds our threshold >=100.4F
        // Deserialized ata from body-temp-event:Key_1_patientID | Value_BodyTemp Object
        // 1|{"timestamp": "2020-11-23T09:03:06.500Z", "temperature": 101.2, "unit": "F"}
        KStream<String, BodyTemp> highBodyTemp_filtered = bodyTempEventsSource
                .filter((key, value) -> // sanity check if value if null or not to avoid Exception
                        value !=null && value.getTemperature()!= null && value.getTemperature() >= 100.4);

        // Task-2[Debug only] print the KStream
        getPrintStream(highBodyTemp_filtered, "high-bodytemp-filtered");

        // ************ E. WINDOWED JOINING (KStream-KStream join) ************************
        // Windowed join is require for KStream-KStream join since KStreams are unbounded
        // Task: Join the pulse rate with body temperature
        // Strategy: Sliding join windows to join KStream 'highPulse_filtered_rekeyed' and kStream 'highBodyTemp_filtered'
        // Recap 'Sliding join windows'
        /*
        *** Sliding join windows

              JoinWindows joinWindow = JoinWindows.of(Duration.ofSeconds(5));
              // Timestamps must be less than or equal to five seconds apart to fall within the same window.

                 - Say, Patient_Record1 (key1:value_1:@10.01) , BodyTemp_Record1 (key2:value_2:@10.03)
                        - Timestamp difference <=5seconds.
                        - Both Record1 and Record2 merged into the same window
                        * key1==key2
                 - Say, Patient_Record2 (key1:value_1:@10.05) , BodyTemp_Record2 (key2:value_2:@10.13)
                        - Timestamp difference >5seconds.
                        - No join operation between Record1 and Record2
                        * key1==key2
         */

        // E1. Setting_01: Define joining parameteres: Specify the Serializer and Deserializer Sherds to be used for joining
        // Define the Serializer and Deserializer settings of key_PatientID and values (Long_PulseCount, BodyTempObject) for KStream-KStream join
        // StreamJoined<Key, Value1, Value2>
        // Why Used StreamJoined, when in Chapter-4 we used Joined?
        // StreamJoined for KStream-KStream join; Joined for KStream-KTable join
        StreamJoined<String, Long,BodyTemp> setting_joinParams_For_filtered_Pulse_and_BodyTemp = StreamJoined.with(
                Serdes.String(), // Key- PatientID // indicates key in both KStream expected to be deserialized as String
                Serdes.Long(), //
                JsonSerdes.BodyTemp()
        );

        // E2. Setting_02: Define Sliding Join Windows
        // Records with timestamp 1min(60) apart of less will fall into the same window and therefore will be joined
        // with a grace period of 10s to wait for delayed events/records : Completeness vs Latency trade-off
        // grace period = allowed lateness in event to keep the window open for delayed event/records
        JoinWindows setting_joinWindows_For_filtered_Pulse_and_BodyTemp = JoinWindows
                .of(Duration.ofSeconds(60))
                .grace(Duration.ofSeconds(10));

        // E3. Setting_03: Join the KStream_high-pulse-count stream and KStream_high-bodytem and return a new Java Object 'CombinedVitals'
        // ValueJoiner(input,input,output)
        ValueJoiner<Long,BodyTemp, CombinedVitals> setting_valueJoiner_For_filtered_Pulse_and_BodyTemp =
                (pulseRate, bodyTemp)-> new CombinedVitals(pulseRate.intValue(),bodyTemp);

        // E4: Perform the actual join between KStreams 'high-pulse-count' and 'high-bodytemp object'
        // KStream<key_PatientID, value_CombinedVitals>
        // Expected Output: [vitals-after-join]: 1, CombinedVitals(heartRate=120, bodyTemp=BodyTemp(timestamp=2020-11-23T09:03:06.500Z, temperature=101.2, unit=F))
        KStream<String,CombinedVitals> vitalsJoined = highPulse_filtered_rekeyed
                .join(
                        highBodyTemp_filtered, // join with 'filtered high-body-temp'
                        setting_valueJoiner_For_filtered_Pulse_and_BodyTemp, // to create a new object after join
                        setting_joinWindows_For_filtered_Pulse_and_BodyTemp, // window size with grace_period (how long to wait for delayed events/records)
                        setting_joinParams_For_filtered_Pulse_and_BodyTemp // serializer and deserializer serdes
                );
        // [Debug only] Print the new KStream after the vitals are joined
        getPrintStream(vitalsJoined, "vitals-after-join");

        // ---------------- F: Write (PRODUCE) the enriched CombinedVital data back to Kafka Sink Stream --------------
        // Why: To make our joinedData 'vitalsJoined' available to downstream consumer
        // We have to write the data back to kafka stream SINK 'alerts'
        // ** The SINK 'alert' topic will pnly be written whenerver our application determines that a patients is at risk for SIRS deseases

        // F1. Write back the data to topic 'alerts'// SINK
        // Which data will be written back?
        // // Expected Output: [vitals-after-join]: 1_patientID_Key_String, Value_CombinedVitals(heartRate=120_Long, bodyTemp_Object=BodyTemp(timestamp=2020-11-23T09:03:06.500Z, temperature=101.2, unit=F))
        vitalsJoined.to(
                "alerts",
                Produced.with(
                        Serdes.String(), // Kafka is a byte-in and byte-out stream.
                                        // So we need to define how the raw joined data will be deserailzied and deserailized in the Kafka topic 'alerts'
                        JsonSerdes.CombinedVitals()
                )
        );


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
}
