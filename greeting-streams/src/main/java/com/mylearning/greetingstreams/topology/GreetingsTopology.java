package com.mylearning.greetingstreams.topology;

import com.mylearning.greetingstreams.domain.Greeting;
import com.mylearning.greetingstreams.serdes.GreetingSerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;

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
    public static String GREETINGS_SPANISH="greetings-spanish";

    // Destination topic name
    public static String GREETINGS_UPPERCASE="greetings-uppercase";
    public static String GREETINGS_SPANISH_UPPERCASE="greetings-spanish-uppercase";

    public static Topology buildTopology(){

        // using StreamBuilder as building block we can define Source-Processor and StreamProcessing Logic and Sink-Processor
        // used StreamBuilder to build a pipeline to read Data from the Kafka-Topic then Performed Data-Enrichment(here modifying from lowercase to uppercase) then writing the data back to the Kafka-Topic
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        /**
         * getStringGreetingKStream() is extracted method from the below statements to get mergedKStream of type KStream<String,String>
         */
        // using java feature type inference and assigning the value to the variable
        // consuming the msg from GREETINGS topic
        // this uses the consumer api
        // if we configured Default Kafka Serdes Key Serializer and Deserializer and Value Serializer and Deserializer in Launcher i.e. GreetingsStreamApp class then no need to specify Key and Value Serializer and Deserializer inside Consumed.With()
        //KStream<String, String> greetingsStream = streamsBuilder.stream(GREETINGS
               // , Consumed.with(Serdes.String(), Serdes.String())
        //);// when used with Consumed.with then first Serdes.String() key deserializer and second Serdes.String() is value deserializer

        // good morning - Buenos días
        // since we have already configured Key and Value Serializer and Deserializer inside Launcher GreetingsStreamApp so no need of specifying Serializer and Deserializer with Consumed.With()
        //KStream<String, String> greetingsSpanish = streamsBuilder.stream(GREETINGS_SPANISH
           //     , Consumed.with(Serdes.String(), Serdes.String())
        //);

        //greetingsStream.print(Printed.<String, String>toSysOut().withLabel("GREETINGS-STREAM-LABEL"));
        //greetingsSpanish.print(Printed.<String, String>toSysOut().withLabel("GREETINGS-SPANISH-LABEL"));

        // using merge operator merging two kafka streams from two different kafka topics so that later down the line we can publish the events into another single kafka stream i.e. combining two kafka streams and perform operations and publish it into another topic
        //KStream<String, String> mergedKStream = greetingsStream.merge(greetingsSpanish);

        /**
         * getStringGreetingKStream() is extracted method from the above statements to get mergedKStream of type KStream<String,String>
         */
        //var mergedKStream = getStringGreetingKStream(streamsBuilder);

        //mergedKStream.print(Printed.<String, String>toSysOut().withLabel("MERGED-KSTREAM-LABEL"));

        //KStream<String, String> modifiedStream = exploreOperatorsStringTypesGreeting(mergedKStream);

        //================================================================================================================================

        /**
         *  below getCustomGreetingKStream() is used for custom Greeting which has message and timestamp so inside custom
         *  Value Serializer and Deserializer is used i.e. GreetingSerializer and GreetingDeserializer
         */
        KStream<String, Greeting> mergedStream = getCustomGreetingKStream(streamsBuilder);
        // anytime the message is read it's going to print to the console with greetingsStream :
        // this way we can analyze or look what is going on once we publish the message into Kafka-topic  and how are KafkaStreams is executing this topology
        // mergedStream.print(Printed.<String,String>toSysOut().withLabel("MERGED-STREAM-LABEL-WITH-STRING-GREETING-SERIALIZER-DESERIALIZER"));
        mergedStream.print(Printed.<String,Greeting>toSysOut().withLabel("MERGED-STREAM-LABEL-WITH-CUSTOM-GREETING-SERIALIZER-DESERIALIZER"));

        KStream<String, Greeting> modifiedStream = exploreOperatorsCustomTypesGreeting(mergedStream);
        //var modifiedStream = exploreErrors(mergedStream);

        // anytime the message is modified it's going to print to the console with modifiedStream : this way we can look analyze how KafkaStreams is executing this topology
        //modifiedStream.print(Printed.<String, String>toSysOut().withLabel("MODIFIED-STREAM-LABEL-WITH-STRING-GREETING-SERIALIZER-DESERIALIZER"));
        modifiedStream.print(Printed.<String,Greeting>toSysOut().withLabel("MODIFIED-STREAM-LABEL-WITH-CUSTOM-GREETING-SERIALIZER-DESERIALIZER"));

        // writing the message to GREETINGS_UPPERCASE topic
        // this uses the producer api
        // since we have already configured Key and Value Serializer and Deserializer inside Launcher GreetingsStreamApp so no need of specifying Serializer and Deserializer with Produced.With()
        // modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(),Serdes.String())); // when used with Produced.with then first Serdes.String() key serializer and second Serdes.String() is value serializer
        // modifiedStream.to(GREETINGS_UPPERCASE); // here we removed Produced.with Serdes.String() because we have configured DEFAULT_KEY_SERDE_CLASS_CONFIG and DEFAULT_VALUE_SERDE_CLASS_CONFIG at Properties config with Serdes.StringSerde.class in launcher from there we Streams get the key and value serializer and deserializer
         modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(),GreetingSerdesFactory.greetingSerde()));
        //modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(),GreetingSerdesFactory.greetingUsingGenerics()));

        return streamsBuilder.build();
    }

    private static KStream<String, String> exploreOperatorsStringTypesGreeting(KStream<String, String> mergedKStream) {
        // inside flatMap returning the list of Key-Value pairs and flatMap flattening the collection and return the individual pairs
        KStream<String, String> modifiedStream = mergedKStream
                //.filter((key,value) -> value.length() > 5)
                /*.peek(((key, value) -> {
                    log.info("after filter > key : {}, value : {}",key,value);
                }))*/
                //.filterNot((key,value) -> value.length() > 5)
                .mapValues((readOnlyKey, value) -> value.toUpperCase())
                .peek(((key, value) -> {
                    log.info("after mapValues > key : {}, value : {}",key,value);
                }))
                //        .map((key,value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()))
                        /*.flatMap((key,value) -> {
                            List<String> newValues = Arrays.asList(value.split(""));
                            List<KeyValue<String, String>> keyValueList = newValues.stream()
                                    .map(val -> KeyValue.pair(key.toUpperCase(), val.toUpperCase()))
                                    .toList();
                            return keyValueList;
                        });*/
                .flatMapValues((readOnlykey,value) -> {
                    List<String> newValues = Arrays.asList(value.split(""));
                    List<String> keyValueList = newValues.stream()
                            .map(String::toUpperCase)
                            .toList();
                    return keyValueList;
                })
                .peek(((key, value) -> {
                    log.info("after flatMapValues > key : {}, value : {}",key,value);
                }))
                ;
        return modifiedStream;
    }


    private static KStream<String, Greeting> exploreOperatorsCustomTypesGreeting(KStream<String, Greeting> mergedStream) {
        // using greetinsStream we can build our Processing logic : we are going to convert the value from lowercase to uppercase
        var modifiedStream= mergedStream
                .peek(((key, value) -> log.info("mergedStream key : {}, value : {}",key,value)))
                //.filter((key, value) -> value.length()>5)   // this will filter out values less than 5 and pass only value length greater than 5
                //.filterNot((key,value) -> value.length()>5) // this will filter out values greater than 5 and pass only value length less than 5
                //.peek((key,value) -> log.info("after filter Key:{}, Value:{}",key,value))   // any changes made in peek() will not be retained or reflected with stream pipeline
                .mapValues((readOnlyKey, value) ->{
                    return new Greeting(value.message().toUpperCase(),value.timeStamp());
                });
        //     .map((key,value) -> KeyValue.pair(key.toUpperCase(),value.toUpperCase()));
                /*.flatMap((key,value) -> {
                    var newValue= Arrays.asList(value.split(""));
                    var keyValueList= newValue.stream()
                            .map(v -> KeyValue.pair(key.toUpperCase(),v.toUpperCase()))
                            .collect(Collectors.toList());
                    return keyValueList;
                }); */
                /*.flatMapValues((readOnlyKey,value) -> {
            var newValue= Arrays.asList(value.split(""));
            return newValue.stream()
                    .map("Hi"::concat)
                    .map(String::toUpperCase)
                    .toList();
        });*/
        modifiedStream.print(Printed.<String, Greeting>toSysOut().withLabel("exploreOperatorsCustomTypesGreeting > MODIFIED-STREAM-WITH-LABEL-WITH-CUSTOM-GREETING-SERIALIZER-DESERIALIZER"));
        return modifiedStream;
    }

    private static KStream<String, String> getStringGreetingKStream(StreamsBuilder streamsBuilder) {
        KStream<String,String> greetingsStream= streamsBuilder.stream(GREETINGS); // Here this way Stream has no clue about what the Serialization and Deserialization going to be but it still needs to get the Serialization and Deserialization and Deserialization from somewhere else so we will configure at Properties config in launcher

        KStream<String,String> greetingsSpanish= streamsBuilder.stream(GREETINGS_SPANISH);

        greetingsStream.print(Printed.<String, String>toSysOut().withLabel("GREETINGS-STREAM-LABEL"));
        greetingsSpanish.print(Printed.<String, String>toSysOut().withLabel("GREETINGS-SPANISH-LABEL"));

        var mergedStream=greetingsStream.merge(greetingsSpanish);
        return mergedStream;
    }

    private static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder streamsBuilder) {
        //KStream<String,String> greetingsStream= streamsBuilder.stream(GREETINGS); // Here this way Stream has no clue about what the Serialization and Deserialization going to be but it still needs to get the Serialization and Deserialization and Deserialization from somewhere else so we will configure at Properties config in launcher

         KStream<String, Greeting> greetingsStream=streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), GreetingSerdesFactory.greetingSerde()));
        //KStream<String, Greeting> greetingsStream=streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), GreetingSerdesFactory.greetingUsingGenerics()));

        // var greetingsSpanish=streamsBuilder.stream(GREETINGS_SPANISH,Consumed.with(Serdes.String(),Serdes.String()));
        KStream<String,Greeting> greetingsSpanish= streamsBuilder.stream(GREETINGS_SPANISH,Consumed.with(Serdes.String(),GreetingSerdesFactory.greetingSerde()));
        //KStream<String,Greeting> greetingsSpanish= streamsBuilder.stream(GREETINGS_SPANISH,Consumed.with(Serdes.String(), GreetingSerdesFactory.greetingUsingGenerics()));

        greetingsStream.print(Printed.<String, Greeting>toSysOut().withLabel("GREETINGS-STREAM-LABEL"));
        greetingsSpanish.print(Printed.<String, Greeting>toSysOut().withLabel("GREETINGS-SPANISH-LABEL"));

        var mergedStream=greetingsStream.merge(greetingsSpanish);
        return mergedStream;
    }
}
