package com.example.topology;

import com.example.models.BodyTemp;
import com.example.models.Pulse;
import com.example.models.VitalTimestampExtractor;
import com.example.serdes.wrapper.JsonSerdes;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class PatientMonitoringTopology {
    // Class Method-01
    public static Topology build(){
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
        getPrintStream(pulseEventsSource,"pulse-events");

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
        getPrintStream(bodyTempEventsSource,"body-temp-events");
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
        // Create a Tumpling window of 60s
        TimeWindows tumblingWindow = TimeWindows.of(Duration.ofSeconds(60)); // Class.method

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

        KTable<Windowed<String>,Long> pulseCounts_aggregated =
                pulseEventsSource // KStream
                        .groupByKey()// doesnt change KStream to KTable
                                    // prerequisit grouping: for aggregation:: count // avoid groupBy() to avoid unnecessary rekey
                        .windowedBy(tumblingWindow) // Changed the KStream to KTable
                                                    // window the pulse event in 60s timeframe
                                                    // This converts the KTable<Key_String, Value_Long> to KTable<Windowed<String>, Value_long>
                        .count(Materialized.as("ss-pulse-counts")); // store the aggregated result into a state-store named "pulse-count"

        // C1.3 [Debug only] Convert the kTable into stream to print the contents into console
        var pulseCount_aggregated_stream = pulseCounts_aggregated.toStream();
        getPrintStream(pulseCount_aggregated_stream, "pulse-counts");

        // **** Have you seen the peculiarity? Producing lots of intermediary as every event/record appears
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
