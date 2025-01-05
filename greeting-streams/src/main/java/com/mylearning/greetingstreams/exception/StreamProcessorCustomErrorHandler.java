package com.mylearning.greetingstreams.exception;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/**
 * ErrorHandlers in KAFKA Streams :::
 *  for custom logic we can build our class implementing this ErrorHandler interfaces and wire that into our application
 * ERROR ::::														Error Handler
 * Deserialization													DeserializationExceptionHandler interface
 * Application Error (Topology)								StreamsUncaughtExceptionHandler interface
 * Serialization														ProductionExceptionHandler interface
 *
 * REPLACE_THREAD(0, "REPLACE_THREAD")
 * this is going to provide a new thread for that error. this is going to constantly read write that failed until that becomes recovered
 *
 * SHUTDOWN_CLIENT(1, "SHUTDOWN_KAFKA_STREAMS_CLIENT"),
 * this is going to shutdown that particular thread which is causing the problem.
 *
 * SHUTDOWN_APPLICATION(2, "SHUTDOWN_KAFKA_STREAMS_APPLICATION");
 * this is going to shutdown the whole kafka-stream application
 *
 * enum StreamThreadExceptionResponse {
 *         REPLACE_THREAD(0, "REPLACE_THREAD"),
 *         SHUTDOWN_CLIENT(1, "SHUTDOWN_KAFKA_STREAMS_CLIENT"),
 *         SHUTDOWN_APPLICATION(2, "SHUTDOWN_KAFKA_STREAMS_APPLICATION");
 *         }
 *
 *  this class is to simulate error at processing time where this class will handle the caught exception i.e. Transient Error
 *  and send the response accordingly.
 *
 * do have this StreamsUncaughtExceptionHandler with this kind of logic if we want to shutdown the application because if we don't
 * shutdown the application it may lead to some kind of corrupted data in our application in those kind of scenarios shutting
 * down the application is best option
 * but alternative is if we want to ignore any error the better approach is to have the handler in the topology itself.
 *
 */
@Slf4j
public class StreamProcessorCustomErrorHandler implements StreamsUncaughtExceptionHandler {
    @Override
    public StreamThreadExceptionResponse handle(Throwable throwable) {

        log.error("StreamProcessorCustomErrorHandler Exception: {}" ,throwable.getMessage(),throwable);

        if( throwable instanceof StreamsException){
            var cause= throwable.getCause();
            if( cause.getMessage().equals("Transient Error")){

                // this is going to replace the new thread and  this is going to retry the same error again and again  and this process is going to continue indefintely
                //  so we do this only if we are sure that exception or error that we are getting are Transient Error and its going to go away after a bit of time
                // return StreamThreadExceptionResponse.REPLACE_THREAD;

                // this will make sure that error is not going to retried  but we are shuting down the client in this case , when error is Transient Error.
                 return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
        }
        //for any other exception we are going to shutdown the application
         return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}
