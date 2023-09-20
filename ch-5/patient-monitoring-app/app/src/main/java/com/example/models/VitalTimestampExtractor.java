package com.example.models;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;
/*
@pulse-event stream
1(patientID)|{"timestamp": "2020-11-23T09:02:00.000Z"}

@body-temp-stream
1(patientID)|{
    "timestamp": "2020-11-23T09:03:06.500Z",    // timestamp is embedded in the payload of the event
    "temperature": 101.2,
    "unit": "F"
    }
 */
// THis is a Custom TimeStamp extractor
// Why? Bcoz event time is embedded in the palyload of each Vital data 'Pulse' and 'BodyTemp'
/*
Time Semantics of Kafka Stream
(a) Event Time semantics - exact/actual time when the event is actualy happended (aka Producer Timestamp)
    - CreateTime - exact time when an event is produced
    - LogAppendTime - time when an event is appended to the topic
if a heartbeat sensor records a pulse at 9.02am, then the event time is 9.02am
- be sure about below two kafka configurations
    (i) @kafka broker level : kafka:29092 is my kafka broker    log.message.timestamp.type (overwrriten with kafka broker's local system time)
    (ii) @kafka topic level             message.timestamp.type (time when an event is appended in the topic)
    ** Topic level configuration takes precedence over broker-level configuration
(b) Ingestion time semantics- Time when the event/record in appended into the Kafka Topic. (approximate event time)
    Concerns
    - The time lag between when an event is created and when an event is appended to a topic

(c) processing time semantics - when Stream Processor is processing the event/records and
        lately sink processor is writing the enhanced event/record back to another kafka topic.
 */

/*
ConsumerRecord<Object, Object> -> is the current consumer record being processed.
        I.e. currently processing 'pulse-event' or 'body-temp' events/records
partitionTime -> kafka stream keeps track of the most recent time it has seen for each partition it consumes from.
                This most recent time is send to the timeextractor through this 'partitionTime' parameter
                ** this is for fallback, in the case timestamp is not available in the record/event payload, or worng
                or timeextractor cant extract the timestamp
 */
public class VitalTimestampExtractor implements TimestampExtractor { // implement TimstampExtractor interface

    /*
    Why ConsumerRecord<Object,Object>?
    Because Source Processor will consume the event/record from 'pulse-event' and 'Body-temp' stream and
    publish these to respective Topics.
     */

    /*
    Note- a defualt timestamp extractor is there named 'WallclockTimestamp';
    this simply returns the kafka broker's local system time
    But we will not use it. Because, we are more interested in 'event-timeseamtics' rather than 'procssing time semantics'
     */
    // long_expected_rutrun_is_time_in_miliseconds extract(current_consumer_record_being_processed, most_recent_timestamp_at_partition_it_consumes_from)
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        // record -> ConsumerRecord<Key,Value>
        // get the Vital object from the record's value
        Vital measurement = (Vital) record.value(); // Vital is an interface
                                // But, why we use Vital interface here, when we have Classes 'Pulse' and 'BodyTemp'
                                // To generialize it further. As The Kafka Stream Source Processor will be consuming from
                                // both 'Pulse' and 'BodyTemp' event stream and both of these has implemented Vital interface
                                // thats why we cast the record.value() as Vital

        // check of the Vital object is not null and it has a timestamp (which is not null)
        // if True, then let the Java Time instance parse this timestamp and return in milliseconds
        // else, we will fallback to the partition's timestamp
        if (measurement!=null && measurement.getTimeStamp()!=null){ // make sure record contains a timestamp before pursing
            String timestamp = measurement.getTimeStamp(); // extract the timestamp
            return Instant.parse(timestamp).toEpochMilli(); // here// expected return is time in milliseconds
        }
        // else, fall back to the partition's timestamp
        /*
        How records without a validtimestamp is handled?
        - (a) Throw an exception and stop processing
        - (b) Fallback to the partition's timestamp *** we will be using it
        - (c) Return a negative timestamp, to allow kafka to skip over it and continue processing
         */
        return partitionTime; // if cant extract timestamp, then fallback strategy
            // it is the most recent timestamp Kafka stream has seen for each partition of consumes from
    }
}
