package com.mylearning.greetingstreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class ExploreKTableTopology {
    // this topic is going to hold bunch of words with key as Alphabet and Word as value to the key.
    public static final String KTABLE_WORDS="ktable-words";

    public static Topology buildTopology(){

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        /**
         * without Materialized.as("words-store") i.e. state-store ktable will get all the key-value data but with state-store
         * i.e. Materialized.as("words-store") ktable buffers the record for certain timeframe and once the timeframe is exhausted
         * ktable publish the message or data to the downstream so we will get only the latest value here

         * WITHOUT Materialized.as("words-store") : shows all the key-value pair data
         * WITH Materialized.as("words-store") shows only the latest key-value pair data in downstream operation.

         * once the message is consumed by this KTable what it does is it keeps buffering for certain time frame and then once again time frame is exhausted then what it will do is
         * it is going to take the latest value for the given key and then publish that message downstream

         * so in this case the KTable waits for certain time frame and buffers the record with in the time frame and once the time frame is exhausted its going to sending
         * that message downstream so in this cases its our filter() operator , toStream() and then print() gets executed based on the data.
         **/
        KTable<String, String> wordsTable=streamsBuilder.table(KTABLE_WORDS, Consumed.with(Serdes.String(),Serdes.String())
        , Materialized.as("words-store")
        );

        /** 10. KTable & Global KTable >> 5. GlobalKTable
         *  when we use Global Ktable then use below code
         **/
        /*GlobalKTable<String, String> wordsGlobalTable=streamsBuilder.globalTable(KTABLE_WORDS, Consumed.with(Serdes.String(),Serdes.String())
                , Materialized.as("words-store")
        );*/

       // log.info("GlobalKTable ::: {}",wordsGlobalTable.queryableStoreName());

        log.info("GlobalKTable ::: {}",wordsTable.queryableStoreName());

        wordsTable.filter((key, value) -> value.length() > 2)
                .toStream()
                .peek((key, value) -> log.info("wordsTable :: key :: {}, value :: {}",key,value))
                .print(Printed.<String,String>toSysOut().withLabel("ktable-words"));

        return streamsBuilder.build();
    }
}
