package com.mylearning.advancedstreams.producer;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.mylearning.advancedstreams.producer.ProducerUtil.publishMessageSync;
import static com.mylearning.advancedstreams.topology.ExploreJoinsOperatorsTopology.ALPHABETS;
import static com.mylearning.advancedstreams.topology.ExploreJoinsOperatorsTopology.ALPHABETS_ABBREVIATIONS;


@Slf4j
public class JoinsMockDataProducer {


    public static void main(String[] args) throws InterruptedException {

        // KTable - ALPHABETS
        var alphabetMap = Map.of(
 //               "A", "A is the first letter in English Alphabets.",
 //               "B", "B is the second letter in English Alphabets."
 //                             ,"E", "E is the fifth letter in English Alphabets."
//                ,
                "A", "A is the FIRST letter in English Alphabets.",
                "B", "B is the SECOND letter in English Alphabets."
        );
         publishMessages(alphabetMap, ALPHABETS);


        // sleep(6000);

        // KStream ALPHABETS_ABBREVIATIONS
        var alphabetAbbrevationMap = Map.of(
                "A", "Apple",
                "B", "Bus."
                ,"C", "Cat."

        );
        publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVIATIONS);

        alphabetAbbrevationMap = Map.of(
                "A", "Airplane",
                "B", "Baby.",
                "E","Elephant"

        );
        // publishing data to alphabets-abbreviations topic
       //  publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVIATIONS);

    }

    private static void publishMessages(Map<String, String> alphabetMap, String topic) {

        alphabetMap
                .forEach((key, value) -> {
                    var recordMetaData = ProducerUtil.publishMessageSync(topic, key,value);
                    log.info("Published the alphabet message : {} ", recordMetaData);
                });
    }



}
