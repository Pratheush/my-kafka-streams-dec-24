package com.mylearning.greetingstreams.exception;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

/**
 *
 * ErrorHandlers in KAFKA Streams :::
 *  for custom logic we can build our class implementing this ErrorHandler interfaces and wire that into our application
 * ERROR ::::														Error Handler
 * Deserialization													DeserializationExceptionHandler interface
 * Application Error (Topology)								StreamsUncaughtExceptionHandler interface
 * Serialization														ProductionExceptionHandler interface
 *
 *
 *  adding a custom DeserializationExceptionHandler class here we are also adding a threshold error counter which
 *  also checks that each instance can handle upto 2 errors only after that application will fail and stop or exit.
 *
 *  since for this GreetingStream Application we have configured or set the StreamThread to 2 by below given property configuration in GreetingStreamApp (a launcher class)
 *  properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,"2");
 *  so that means there will be 2 StreamThread instances per application and each StreamThread instance handles the tasks
 *  and when each StreamThread instance reaches the threshold value of errorCounter upto 2 then the application will fail and exit
 *
 *   // setting up the number of StreamThreads manually
 *         // Runtime.getRuntime().availableProcessors(); // we can get the number of threads to set for number os StreamThreads.
 *         properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,"2");
 *
 *          Also configuring and setting up the Custom DeserializationExceptionHandler for our Kafka-Stream Application
 *         properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, GreetingStreamsDeserializationExceptionHandler.class);
 */
@Slf4j
public class GreetingStreamsDeserializationExceptionHandler implements DeserializationExceptionHandler {

    // adding errorCounter Threshold value i.e. how many error can handle in this instance
    int errorCounter = 0;
    @Override
    public DeserializationHandlerResponse handle(ProcessorContext processorContext, ConsumerRecord<byte[], byte[]> consumerRecord, Exception e) {
        log.error(" GreetingStreamsDeserializationExceptionHandler : {} , and Kafka Record is : {}",e.getMessage(),consumerRecord,e);
        log.info("ErrorCounter : {}",errorCounter);
        if(errorCounter<2){
            errorCounter++;
            return DeserializationHandlerResponse.CONTINUE;
        }
        return DeserializationHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
