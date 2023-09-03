package com.example;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

public class CryptoTopology {

    // Why static? Because App.main() is a static method and its gonna call this Topology.build() method
    public static Topology build(){
        // Add a StreamBuilder to construct a Kafka topology
        StreamsBuilder builder = new StreamsBuilder();

        // A. Source Processor
        // Add a source processor to the topology to read tweets from the Topic "tweets"
        // KStream<Key,Value> ; KSteam is an interface
        // KStream<byte[],byte[]>, means record/events keys and values coming out of the topic "tweets" are being encoded as byte arrays
        // But tweet records are actually encoded as JSON object by the source connector; then what gives these bytes?
        // Bcoz, Kafka Streams, by defaults, represent data flows through our application as byte arrays; and its a sensible approach as this doesnt impose any particular data format on its client
        // and also fast(required less memory and CPU cycles on the brokers to transfer a raw byte stream over the network)
        // So, Who will serialize or deserailzie these byte streams?
        // Kafka client, including kafka stream applications are responsible to do so to work with high-level objects or formats i.e. Strings, JSON, Avro etc
        KStream<byte[], byte[]> source = builder.stream("tweets");

        // B. STREAM PROCESSOR - to print the KStream
        // print the KStream
        source.print(Printed.<byte[],byte[]>toSysOut().withLabel("tweet-stream"));

        // Return the topology to App.main()
        Topology topology = builder.build();
        return topology;

    }

}
