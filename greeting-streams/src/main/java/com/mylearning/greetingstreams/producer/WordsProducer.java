package com.mylearning.greetingstreams.producer;

import lombok.extern.slf4j.Slf4j;

import static com.mylearning.greetingstreams.producer.ProducerUtil.publishMessageSync;


@Slf4j
public class WordsProducer {

    static String WORDS = "ktable-words";
    public static void main(String[] args) throws InterruptedException {

        // When key is null then this will happen
        // 13:28:06.994 [ktable-ab67a044-caa8-4d62-a74b-053a02c988e2-StreamThread-1]
        // WARN  o.a.k.s.k.internals.KTableSource - Skipping record due to null key. topic=[ktable-words] partition=[0] offset=[55]
        String key = "A"; // key should be "A" so that Ktable will not ignore the record with key

        var word = "Apple";
        var word1 = "Alligator";
        var word2 = "Ambulance3";

        var recordMetaData = publishMessageSync(WORDS, key,word);
        log.info("Published the alphabet message word : {} ", recordMetaData);

        var recordMetaData1 = publishMessageSync(WORDS, key,word1);
        log.info("Published the alphabet message word1 : {} ", recordMetaData1);

        var recordMetaData2 = publishMessageSync(WORDS, key,word2);
        log.info("Published the alphabet message word2 : {} ", recordMetaData2);

        var bKey = "B";

        var bWord1 = "Bus";
        var bWord2 = "Baby";
        var bWord3 = "BobMarley3";
        var recordMetaData3 = publishMessageSync(WORDS, bKey,bWord1);
        log.info("Published the alphabet message bWord1: {} ", recordMetaData3);

        var recordMetaData4 = publishMessageSync(WORDS, bKey,bWord2);
        log.info("Published the alphabet message  bWord2: {} ", recordMetaData4);

        var recordMetaData5 = publishMessageSync(WORDS, bKey,bWord3);
        log.info("Published the alphabet message  bWord2: {} ", recordMetaData5);

    }

}
