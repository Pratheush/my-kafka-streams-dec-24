package com.mylearning.advancedstreams.topology;


import com.mylearning.advancedstreams.domain.AlphabetWordAggregate;
import com.mylearning.advancedstreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ExploreAggregateOperatorsTopology {


    public static final String AGGREGATE = "aggregate";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputStream = streamsBuilder
                .stream(AGGREGATE,
                        Consumed.with(Serdes.String(),Serdes.String()));

        inputStream
                .print(Printed.<String,String>toSysOut().withLabel(AGGREGATE));
        /**
         * groupByKey() takes the key automatically from the actual record and then apply grouping on it
         * so if the key is we would like to change from the actual Kafka Record that's when we use operator groupBy()
         * groupBy() accepts the KeyValueMapper basically its a key selector so the advantage is that we can provide what the new key is going to be
         * we can alter the Key Also . For some use-cases there might be no key in Kafka Records Streaming from the Kafka Topic
         * in this case we have the key as A and B in some cases there might not be any key in those kind of scenarios we can use
         * groupBy() operator to change a key or even if there is an existing key and we would like to change the key to different value then use groupBy() operator
         * anytime we perform groupBy() its recommended to provide the type also >> here  Grouped.with(Serdes.String(), Serdes.String())
         **/
        KGroupedStream<String,String> groupedString = inputStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String())) // here key is same as in Kafka Records from Internal Kafka Topic
                //        .groupBy((key, value) -> value.toUpperCase() ,                // here key is now changed value has become key and below type of key-value is provided which is recommended. so running producer for one time application will get Apple -1,Alligator-1,Ambulance-1 so Apple is key and 1 will be count.
                  //              Grouped.with(Serdes.String(),Serdes.String())) // group type is defined for groupBy() operation.
                ;

        /**
         * 2. What is Aggregation & How it works
         * 3. Aggregation using count operator
         */
         exploreCount(groupedString);

        /**
         * 4. Aggregation using reduce operator
         */
        exploreReduce(groupedString);

        /**
         * 5. Aggregation using aggregate operator
         */
        // exploreAggregate(groupedString);

        return streamsBuilder.build();
    }

    /**
     * advantages of using Materialized views is saving the state of the aggregated operation
     * if we don't use our own materialized view then Kafka Streams does is that internal kafka topic and maintains the data in the kafka topic
     * but there is no way to query the data when we are using this approach i.e. when we don't use Materialized views.
     *
     * but when we are using Materialized views then we can query the data from the internal topic.
     * @param groupedStream
     */
    private static void exploreCount(KGroupedStream<String, String> groupedStream) {

        // when we do something like this what Kafka Stream does is that it creates an internal Kafka Topic and maintains the data in the Kafka Topic
        // but there is no way to query the data if we are using this approach i.e. without using Materialized
        // if we are using Materialized then instead of creating Internal Kafka Topic of its own its going to create state store and maintain the data in the internal Kafka Topic

        KTable<String,Long> countByAlphabet = groupedStream
                //.count(Named.as("count-per-alphabet"))
                .count(Named.as("COUNT-PER-ALPHABET")
                , Materialized.as("COUNT-PER-ALPHABET-STORE") // so here instead of creating internal kafka topic of its own it's going to create state store as we mentioned and maintain the data in the internal kafka topic.
                );

        countByAlphabet
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel("WORDS-COUNT-PER-ALPHABET"));

    }

    /**
     *  reduce() operator is used to reduce multiple values to a single value that shares the same key
     *  behind the scenes Kafka Stream did create  internal Kafka Topic and stored the complete state over there
     *  for FaultTolerance and retention so anytime we restart our kafka-streaming application this is one of
     *  the ways getting the previous state and load the state into our kafka-streaming application.
     *  reduce() operator returns KTable of Type as Value Type. i.e. here value of KGroupedStream is of Type String so reduce() returns KTable of type String as value is String type.
     *  reduce() operator cannot return other type other than value type used in there reduce() operator
     *  so if we want to reduce() operator to return length of the value then it will give error, but it can be done using aggregate() operator
     */
    private static void exploreReduce(KGroupedStream<String, String> groupedStream){
        KTable<String,String> reducedKtable=groupedStream
                                        .reduce((previousValue, currentValue) -> {
                                           log.info("exploreReduce() PreviousValue :: {} , CurrentValue :: {}",previousValue,currentValue);
                                           return  previousValue.toUpperCase() + "-" + currentValue.toUpperCase();
                                        }
                                        , Materialized.<String,String,KeyValueStore<Bytes,byte[]>>as("REDUCED-WORDS-STORE")
                                                        .withKeySerde(Serdes.String())
                                                        .withValueSerde(Serdes.String())
                                        );
        reducedKtable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("REDUCED-WORDS"));
    }

    private static void exploreAggregate(KGroupedStream<String, String> groupedStream){
        /**
         *  Initializer is java bean which is going to represent JSON that has three properties, and we are instantiating  new instance of it. Type here is initializer.
         *   creating new empty instance
         */
        Initializer<AlphabetWordAggregate> alphabetWordAggregateInitializer= AlphabetWordAggregate::new;

        /**
         *  this is the actual code which is going to perform aggregation
         *  as we are getting new value we are calling updateNewEvents()
         *  aggregator :: where we are updating the runningCount and update the array-value or list values with the new value as the new values comes in .
         *  Aggregator<Key-Type,Value-Type, OutPut-Type>
         */
        Aggregator<String,String,AlphabetWordAggregate> alphabetWordAggregateAggregator=
                (key, value, alphabetWordAggregate) -> alphabetWordAggregate.updateNewEvents(key, value);
        /**
         * since we are changing the type from one to another because we are changing from type String to AlphabetWordAggregate
         * the better option is to provide Materialized view. Materialized is one of the option of providing our own State Store instead of Kafka taking care to create that State Store for us.
         * Materialized.<String,AlphabetWordAggregate >as("aggregated-store") first is key second is the value  third is providing what kind of state store we are going to use.
         * materializing the aggregated value to a stateStore
         * as this is needed anytime the application is restarted and the app needs to reconstruct whole stream / state
         * the reason why Materialized is used in this use-case  not for the other one is that because the type here is going to be little different because the value is going to be new object or a new type.
         * it's not going to be String anymore .that is why we are using materialized . so in aggregate operator we have to use Materialized view but in count and reduce operator if we don't use Materialized then there won't be any problems.
         * advantage of using Materialized views when saving the state of the Aggregated Operation
         *
         * Earlier in exploreCount() and exploreReduce() function we didn't provide Materialized so what KafkaStreams behind the scenes did that it created an internal Kafka Topic
         * and store the complete state over there in the Internal Topic which is created for fault Tolerance and Retention.
         *  But In aggregate() operation we are taking control of it.
         *
         *  Materialized.<Key-Type, Value-Type, What-Kind-of-StateStore-We-are-going-to-Use> and KeyValueStore<Bytes,byte[]> here key is going to be Bytes type and byte[] is going to be value type.
         *  Materialized.<String,AlphabetWordAggregate, KeyValueStore<Bytes,byte[]>>. Here KeyValueStore is a kind of State Store we are going to use
         */
        KTable<String,AlphabetWordAggregate>alphabetWordAggregateKTable=groupedStream
                                            .aggregate(alphabetWordAggregateInitializer,
                                                    alphabetWordAggregateAggregator,
                                                    Materialized.<String,AlphabetWordAggregate, KeyValueStore<Bytes,byte[]>>as("AGGREGATED-WORDS-STORE")
                                                            .withKeySerde(Serdes.String()) // specifying key serde
                                                            .withValueSerde(SerdesFactory.alphabetWordAggregate()) // specifying value serde
                                                    );
        alphabetWordAggregateKTable
                .toStream()
                .print(Printed.<String,AlphabetWordAggregate>toSysOut().withLabel("AGGREGATED-WORDS"));
    }

}
