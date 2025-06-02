package com.mylearning.advancedstreams.topology;

import com.mylearning.advancedstreams.domain.Alphabet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.LocalTime;

@Slf4j
public class ExploreJoinsOperatorsTopology {


    // creating a KTable out of this because A is always going to be the first letter in the alphabet
    public static final String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    // creating a KStream out of this because this can have different alphabets with different abbreviations
    public static final String ALPHABETS_ABBREVIATIONS = "alphabets_abbreviations"; // A=> Apple

    public static final String ALPHABET_TOPIC="alphabets-join";

    public static final String JOINED_STREAM="JOINED-STREAM";

    private ExploreJoinsOperatorsTopology(){}


    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // joinKStreamWithKTable(streamsBuilder);
        // joinKStreamWithGlobalKTable(streamsBuilder);
        // joinKTableWithKTable(streamsBuilder);
        // joinKStreamWithKStream(streamsBuilder);
        // joinKStreamWithKStreamWithLeftJOIN(streamsBuilder);
         joinKStreamWithKStreamWithOuterJOIN(streamsBuilder);
        return streamsBuilder.build();
    }

    /**
     * joins will get triggered if there is a matching record for the same key.
     * to achieve a resulting  data model like this, we would need a ValueJoiner.
     * this is also called innerJoin.
     * Join won't happen if the records from topics don't share the same key.
     *
     * so in case of join operation KStream with KTable
     * new events into the KTable doesn't trigger any join
     * but new events into the KSTREAM always trigger join if there is matching key is found in KTable
     * @param streamsBuilder
     */
    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder){

        KStream<String,String> alphabetAbbrevationsKStream=streamsBuilder
                        .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS_ABBREVIATIONS-KSTREAM"));


        KTable<String,String> alphabetKTable=streamsBuilder
                .table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String())
                        , Materialized.as("ALPHABETS-STORE"));

        alphabetKTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("ALPHABETS-KTABLE"));


        //<V1> – first value type <V2> – second value type <VR> – joined value type
        ValueJoiner<String, String, Alphabet> alphabetValueJoiner = Alphabet::new;

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream
                                    .join(alphabetKTable,alphabetValueJoiner);

        // [alphabets-with-abbreviations]: A, Alphabet[abbreviation=Apple, description=A is the first letter in English Alphabets.]
        // [alphabets-with-abbreviations]: B, Alphabet[abbreviation=Bus, description=B is the second letter in English Alphabets.]
        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel("ALPHABETS-WITH-ABBREVIATIONS"));
    }

    /**
     * Here it works same as KStream and KTable Joining
     * However when we are joining KStream with GlobalKTable then We need KeyValueMapper and ValueJoiner the reason is that GlobalKTable is the representation of all the data that's part of the KAFKA Topic
     * it's not about a specific instance holding set of keys based on partition that particular task interacts with it's going to have whole representation in those kind of scenarios we need to provide
     * KeyValueMapper that's going to represent what the key is going to be in this case
     * @param streamsBuilder
     */

    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder){

        var alphabetAbbrevationsKStream=streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS_ABBREVIATIONS-KSTREAM"));


        var alphabetGlobalKTable=streamsBuilder
                .globalTable(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabets-store"));

        // GlobalKTable has no toStream() method
        /*alphabetGlobalKTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));*/


        /**
         * when we are joining KStream with GlobalKTable then We need KeyValueMapper and ValueJoiner the reason is that GlobalKTable is the representation of all the data that's part of the KAFKA Topic
         * it's not about a specific instance holding set of keys based on partition that particular task interacts with it's going to have whole representation in those kind of scenarios we need to provide
         * KeyValueMapper that's going to represent what the key is going to be in this case
         */
        // <K> – key type from KStream <V> – value type from KStream <VR> – mapped value type ; :::: here below this leftKey is key from KStream i.e. alphabetAbbrevationsKStream
        KeyValueMapper<String,String,String> keyValueMapper= (leftKey, alphabetAbbrevationValue) -> leftKey;
       /* KeyValueMapper<String,String,String> keyValueMapper= (leftKey, rightKey) -> {
            if (leftKey.equals(rightKey)) return leftKey;
            else return  rightKey;
        };*/

        //<V1> – first value type from KStream <V2> – second value type from GlobalKTable <VR> – joined value type
        ValueJoiner<String, String, Alphabet> alphabetValueJoiner= Alphabet::new;
        // ValueJoiner<String, String, Alphabet> alphabetValueJoiner= (stringAlphabetAbrevationValue, stringAlphabetDescriptionValue) -> new Alphabet(stringAlphabetAbrevationValue, stringAlphabetDescriptionValue);

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream
                .join(alphabetGlobalKTable,keyValueMapper,alphabetValueJoiner);

        /**
         * since new events in KStream trigger join operation but new events in GlobalKTable does not trigger join operation.
         *  whenever data is present or arrived in ALPHABETS_ABBREVIATIONS topic then only join operation is triggered since only two data is present in KStream so
         *  with the record with matching key from KTable two times result is produced
         */
        // [JOINED-STREAM]: A, Alphabet[abbreviation=Apple, description=A is the first letter in English Alphabets.]
        // [JOINED-STREAM]: B, Alphabet[abbreviation=Bus, description=B is the second letter in English Alphabets.]
        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel(JOINED_STREAM));
    }

    /**
     * In this usecase either side of the data or new event is going to trigger join operation, in our case :::: alphabets_abbreviations table and alphabet table both are KTables so irrespective of whether the data is going to
     * be sent to this alphabets_abbreviations table or alphabet table there will be join triggered.
     * if new event is published in first KTable and no new events Published in second KTable then join operation will be triggered and only that number of events producer results with the matching key
     * if new event is published in both KTable then join operation will produce result with number of total events from both KTable with matching key will produce results.
     * @param streamsBuilder
     */
    private static void joinKTableWithKTable(StreamsBuilder streamsBuilder){

        KTable<String,String> alphabetAbbrevationsKTable=streamsBuilder
                .table(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.<String, String, KeyValueStore<Bytes,byte[]>>as("ALPHABETS-ABBREVIATIONS-STORE"));

        alphabetAbbrevationsKTable
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS-ABBREVIATIONS-KTABLE"));


        KTable<String,String> alphabetKTable=streamsBuilder
                .table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("ALPHABETS-STORE"));

        alphabetKTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("ALPHABETS-KTABLE"));


        //<V1> – first value type <V2> – second value type <VR> – joined value type
        ValueJoiner<String, String, Alphabet> alphabetValueJoiner= Alphabet::new;

        KTable<String,Alphabet> joinedStream=alphabetAbbrevationsKTable
                .join(alphabetKTable,alphabetValueJoiner);

        /**
         * irrespective of whether the data or event is present in ALPHABETS_ABBREVIATIONS Topic (KTable1) or
         * data is present in ALPHABETS Topic (KTable2) it's going to trigger join operation
         * thus getting output produced for that number of events occurred i.e. if two events arrived or present in topic1 then it will trigger join operation two times and
         * when two more events arrived or present in topic2 then it will trigger join operation again two more times thus produced output will be four times.
         */
        // [JOINED-STREAM]: A, Alphabet[abbreviation=Apple, description=A is the first letter in English Alphabets.]
        // [JOINED-STREAM]: B, Alphabet[abbreviation=Bus, description=B is the second letter in English Alphabets.]
        // [JOINED-STREAM]: B, Alphabet[abbreviation=Bus, description=B is the second letter in English Alphabets.]
        // [JOINED-STREAM]: A, Alphabet[abbreviation=Apple, description=A is the first letter in English Alphabets.]
        joinedStream
                .toStream()
                .print(Printed.<String, Alphabet>toSysOut().withLabel(JOINED_STREAM));
    }

    private static void joinKStreamWithKStream(StreamsBuilder streamsBuilder){
        // Primary
        var alphabetAbbrevationsKStream=streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS-ABBREVIATIONS-KSTREAM" +"::"+streamTimeStamp()));

        // Secondary
        var alphabetKStream=streamsBuilder
                .stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetKStream
                .print(Printed.<String, String>toSysOut().withLabel("ALPHABETS-KSTREAM" +"::"+streamTimeStamp()));

        /**
         * Join KStream - KStream ::::
         * The KStream-KStream join is little different compared to the other ones.
         * A KStream is an infinite stream which represents a log of everything that happened
         *
         * JoinWindows:::
         * It is expected that they both share the same key, and also it should be in certain time window( there is a time window
         * defined within the time window those events should be part of the KStream events )
         *
         * so by default any records that gets produced in the KAFKA Topic gets a timestamp attached to it
         *
         * what is the type of join-params
         * StreamJoined<K,V1,V2> this class using which we can provide what the key-value type and value type of other stream is going to be
         *
         * if the primary stream begins and a window defined is the 5-second window (here 5-second window is specified as JoinWindows ) which is back
         * and forth which means like if the primary or first event occurred at o time is 5:00:00 of the event in primary stream then secondary stream
         * event should be occurring comes in between 4:59:56 (4pm59 minutes and 56 seconds) and 5:00:05 (5 pm 00 minutes and 05 seconds )within that window back and forth
         * if the second event comes in the secondary stream then two events or messages from two KStreams will be joined when they both have same matching key
         */

        //<V1> – first value type <V2> – second value type <VR> – joined value type
        ValueJoiner<String, String, Alphabet> alphabetValueJoiner= Alphabet::new;

        JoinWindows fiveSecondWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        /**
         *
         * StreamJoined is a class that provides utility methods to define the serdes (serializer/deserializer) used when joining two KStreams.
         * Serdes.String() is a built-in serde for handling strings. It’s being used here for both the keys and values of the streams that are been joined.
         * StreamJoined.with() is a static factory method that creates a new StreamJoined instance with the specified key, value, and other serdes.
         * In this case, streamJoined is an instance of StreamJoined configured to use String serdes for both keys and values of the joining streams/tables.
         *
         *  StreamJoined this is the class using which we can provide what the key-value and the return type is going to be
         *  first argument is the Key Type we are dealing with Alphabet-Abbreviations-Kstream
         *  second argument is the Value Type we are dealing with Alphabet-Abbreviations-Kstream
         *  the third argument is the type of the value we are dealing with Alphabet-Stream
         */
        StreamJoined<String, String, String> joinedParams = StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String());

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream
                .join(alphabetKStream,alphabetValueJoiner,fiveSecondWindow,joinedParams);

        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel(JOINED_STREAM+"::"+streamTimeStamp()));

    }

    private static void joinKStreamWithKStreamWithLeftJOIN(StreamsBuilder streamsBuilder){
        KStream<String,String> alphabetKStream=streamsBuilder
                                        .stream(ALPHABETS,
                                            Consumed.with(Serdes.String(),Serdes.String()));

        alphabetKStream
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS-KSTREAM"));

        KStream<String,String>  alphabetAbbrevationsKStream=streamsBuilder
                                                        .stream(ALPHABETS_ABBREVIATIONS,
                                                                Consumed.with(Serdes.String(),Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS-ABBREVIATIONS-KSTREAM"));

        ValueJoiner<String,String,Alphabet> alphabetValueJoiner=Alphabet::new;

        StreamJoined<String,String,String> paramJoins=StreamJoined.with(Serdes.String(),Serdes.String(),Serdes.String())
                .withName("ALPHABETS-JOINS")
                .withStoreName("ALPHABETS-JOINS");

        JoinWindows fiveSecondWindow=JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream
                                                    .leftJoin(alphabetKStream,
                                                        alphabetValueJoiner,
                                                        fiveSecondWindow,
                                                        paramJoins);

        /**
         * [JOINED-STREAM]: C, Alphabet[abbreviation=Cat., description=null]
         * if there is no matching record on the right side, then the join will be triggered with null value for the right side value
         * here right is Alphabet with key = A, value = A is the first letter in English Alphabets
         * here left is Alphabet-Abbreviations with key = A, value = Apple
         * when both sides matching records then it will trigger join work as usual.
         */
        joinedStream
                .print(Printed.<String,Alphabet>toSysOut().withLabel(JOINED_STREAM));

    }

    /**
     * StreamsBuilder is used to construct a topology of Kafka Streams.
     *
     * alphabetKStream is created by consuming messages from the Kafka topic named ALPHABETS using the stream() method of StreamsBuilder.
     * It's configured to deserialize keys and values as strings.
     * Similarly, alphabetAbbrevationsKStream is created by consuming messages from the Kafka topic named ALPHABETS_ABBREVATIONS
     *
     * ValueJoiner Definition:
     *
     * A ValueJoiner named alphabetValueJoiner is defined. It's a functional interface used to merge the values of the two streams into an instance of Alphabet.
     * The Alphabet::new method is likely a constructor reference used to create an Alphabet object.
     *
     * Configuring Serdes (Serializer/Deserializer):
     * Kafka Streams requires Serdes to serialize and deserialize keys and values when reading from and writing to topics.
     * StreamJoined is used to specify the Serdes for keys and values of both input streams.
     * In the code, Serdes.String() is used for both keys and values, indicating that the keys and values of the streams are expected to be strings.
     *
     * StreamJoined Configuration:
     * StreamJoined is a builder class that configures parameters for joining kafka streams
     * It's configured with key, value, and store serdes (serializer/deserializer) using Serdes.String() for both keys and values.
     * withName() and withStoreName() methods are used to name the joined stream and specify the store name respectively
     * withName(ALPHABET_TOPIC) is used to set the name of the joined stream. This name will be used internally within Kafka Streams.
     * Kafka Streams allows you to store intermediate results of stream processing in state stores. so specify the store-name to store intermediate results of stream processing
     *
     * Join Window Configuration:
     * JoinWindows defines the window settings for the join operation. In this case, a window of 5 seconds is set with no grace period
     *
     * outerJoin()  takes the following parameters
     * alphabetValueJoiner: A function that merges values from both streams into an Alphabet object.
     * fiveSecondWindow: The window configuration for the join operation.
     * paramJoins: Parameters for joining streams, including serdes and store names.
     *
     * KeyValueMapper is a functional interface provided by Kafka Streams. It defines a method apply that takes two parameters - a key and a value - and returns a new key.
     * public interface KeyValueMapper<K, V, R> {
     *     R apply(K key, V value);
     * }
     * The join operation combines each record from the KStream (alphabetAbbrevationsKStream) with the corresponding record from the GlobalKTable (alphabetKTable).
     * However, for this join operation, a mapper is needed to match records from the KStream with records from the GlobalKTable.
     * The KeyValueMapper is used to specify how the key from the KStream (alphabetAbbrevationsKStream) should be matched with the key from the GlobalKTable (alphabetKTable).
     *
     *  KeyValueMapper<String,String,String> keyValueMapper= (leftKey, rightKey) -> leftKey;
     *  This lambda simply takes the key from the left side (KStream) and uses it as the key for the join operation.
     *  means that the records from the KStream are joined with the records from the GlobalKTable based solely on the key from the KStream.
     *  KeyValueMapper is crucial in specifying how keys should be matched between a KStream and a GlobalKTable in the join operation.
     *
     *  KeyValueMapper<String,String,String> keyValueMapper= (leftKey, rightKey) -> {
     *             if (leftKey.equals(rightKey)) return leftKey;
     *             else return  rightKey;
     *         };
     * @param streamsBuilder
     */

    private static void joinKStreamWithKStreamWithOuterJOIN(StreamsBuilder streamsBuilder){

        KStream<String,String> alphabetKStream=streamsBuilder
                .stream(ALPHABETS,
                        Consumed.with(Serdes.String(),Serdes.String()));

        alphabetKStream
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS-KSTREAM"));

        KStream<String,String>  alphabetAbbrevationsKStream=streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS,
                        Consumed.with(Serdes.String(),Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS-ABBREVIATIONS-KSTREAM"));

        ValueJoiner<String,String,Alphabet> alphabetValueJoiner=Alphabet::new;

        StreamJoined<String,String,String> paramJoins=StreamJoined.with(Serdes.String(),Serdes.String(),Serdes.String())
                .withName(ALPHABET_TOPIC)
                .withStoreName(ALPHABET_TOPIC)
                ;

        JoinWindows fiveSecondWindow=JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream.outerJoin(alphabetKStream,
                alphabetValueJoiner,
                fiveSecondWindow,
                paramJoins);

        joinedStream
                .print(Printed.<String,Alphabet>toSysOut().withLabel(JOINED_STREAM));

    }

    private static String streamTimeStamp(){
        LocalTime now = LocalTime.now();
        return now.toString();
    }


}
