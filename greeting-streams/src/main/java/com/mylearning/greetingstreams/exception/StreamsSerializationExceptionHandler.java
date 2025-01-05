package com.mylearning.greetingstreams.exception;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

/**
 * how to handle error at serialization part. when we are trying to write the data  into the kafka topic from your topology.
 * and errors can be handled by ProductionExceptionHandler interface.
 *
 * ErrorHandlers in KAFKA Streams :::
 *  for custom logic we can build our class implementing this ErrorHandler interfaces and wire that into our application
 * ERROR ::::														Error Handler
 * Deserialization													DeserializationExceptionHandler interface
 * Application Error (Topology)								StreamsUncaughtExceptionHandler interface
 * Serialization														ProductionExceptionHandler interface
 *
 *  CONTINUE(0, "CONTINUE"),
 *  in case of error then continue processing the remaining records
 *
 *  FAIL(1, "FAIL");
 *  in case of error then fail the application completely.
 *
 *   enum ProductionExceptionHandlerResponse {
 *   CONTINUE(0, "CONTINUE"),
 *   FAIL(1, "FAIL");
 *   }
 *
 * Under what scenario this ProductionExceptionHandler will come into play or under what scenario we may run into SerializationException
 * lets say we have the transform record in our stream application which is too big to be published in those kind of scenario
 * we can run into serializationException. or it could be a transient error while publishing the record into Kafka-Topic
 * meaning our kafka cluster is temporarily down or it could be rebalancing its happening so that our application is not able to
 * communicate to kafka-Cluster in those scenarios we may run into Serialization Exception.
 *
 */

@Slf4j
public class StreamsSerializationExceptionHandler implements ProductionExceptionHandler {
    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> producerRecord, Exception e) {
        log.error("StreamsSerializationExceptionHandler Exception {} and ProducerRecord {} ",e.getMessage(),producerRecord,e);
        if ( e instanceof RecordTooLargeException){
            return ProductionExceptionHandlerResponse.FAIL;
            //return ProductionExceptionHandlerResponse.CONTINUE;
        }
        return ProductionExceptionHandlerResponse.CONTINUE; // this means that we don't care about any problems in publishing the records but we still want to constantly publish new records and execute by topology
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
