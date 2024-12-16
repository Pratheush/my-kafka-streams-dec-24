package com.mylearning.greetingstreams.topology;

import com.mylearning.greetingstreams.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Topology is a class in Kafka Streams that which basically holds the whole flow of the Kafka Streams
 * this holds the whole processing logic for our Kafka Streams Application
 *
 * serdes is a factory class in kafka streams that takes care of serialization and deserialization of key and value this is little different from Kafka Consumer and Producer Api
 * because when we are building Kafka Consumer and Producer API  we specifically call out the serializer and deserializer properties and provide the appropriate class for serialization and deserialization
 *
 * .\bin\windows\kafka-console-consumer.bat  --bootstrap-server localhost:9092 --topic greetings-uppercase
 */
@Slf4j
public class GreetingsTopology {

    // Source topic name
    public static String GREETINGS="greetings";

    // Destination topic name
    public static String GREETINGS_UPPERCASE="greetings-uppercase";
    public static String GREETINGS_SPANISH_UPPERCASE="greetings-spanish-uppercase";

    public static Topology buildTopology(){

        // using StreamBuilder as building block we can define Source-Processor and StreamProcessing Logic and Sink-Processor
        // used StreamBuilder to build a pipeline to read Data from the Kafka-Topic then Performed Data-Enrichment(here modifying from lowercase to uppercase) then writing the data back to the Kafka-Topic
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        // using java feature type inference and assigning the value to the variable
        // consuming the msg from GREETINGS topic
        // this uses the consumer api
        KStream<String, String> greetingsStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));// when used with Consumed.with then first Serdes.String() key deserializer and second Serdes.String() is value deserializer

        greetingsStream.print(Printed.<String, String>toSysOut().withLabel("GREETINGS-STREAM-LABEL"));

        KStream<String, String> modifiedStream = greetingsStream.mapValues((readOnlyKey, value) -> value.toUpperCase());
        // var mergedStream = getStringGreetingKStream(streamsBuilder);
        //var mergedStream = getCustomGreetingKStream(streamsBuilder);

        // anytime the message is read it's going to print to the console with greetingsStream :
        // this way we can analyze or look what is going on once we publish the message into Kafka-topic  and how are KafkaStreams is executing this topology
        // mergedStream.print(Printed.<String,String>toSysOut().withLabel("mergedStream"));
        //mergedStream.print(Printed.<String,Greeting>toSysOut().withLabel("mergedStream"));

        // var modifiedStream = exploreOperators(mergedStream);
        //var modifiedStream = exploreErrors(mergedStream);

        // anytime the message is modified it's going to print to the console with modifiedStream : this way we can look analyze how KafkaStreams is executing this topology
        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("MODIFIED-STREAM-LABEL"));
        //modifiedStream.print(Printed.<String,Greeting>toSysOut().withLabel("modifiedStream"));

        // writing the message to GREETINGS_UPPERCASE topic
        // this uses the producer api
         modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(),Serdes.String())); // when used with Produced.with then first Serdes.String() key serializer and second Serdes.String() is value serializer
        // modifiedStream.to(GREETINGS_UPPERCASE); // here we removed Produced.with Serdes.String() because we have configured DEFAULT_KEY_SERDE_CLASS_CONFIG and DEFAULT_VALUE_SERDE_CLASS_CONFIG at Properties config with Serdes.StringSerde.class in launcher from there we Streams get the key and value serializer and deserializer
        // modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(),GreetingSerdesFactory.greeting()));
        //modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(),GreetingSerdesFactory.greetingUsingGenerics()));

        return streamsBuilder.build();
    }

}
