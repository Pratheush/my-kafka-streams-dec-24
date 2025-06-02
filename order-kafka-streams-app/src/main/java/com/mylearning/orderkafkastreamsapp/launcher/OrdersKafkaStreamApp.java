package com.mylearning.orderkafkastreamsapp.launcher;


import com.mylearning.orderkafkastreamsapp.topology.OrdersTopology;
import com.mylearning.orderkafkastreamsapp.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class OrdersKafkaStreamApp {


    public static void main(String[] args) {

        // create an instance of the topology
        var orderTopology= OrdersTopology.buildTopology();
        // var orderTopology= OrdersTopologyByTutorial.buildTopology();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-app"); // consumer group
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // read only the new messages

        // The value of threads is controlled by the num.stream.threads. property
        // setting up the number of StreamThreads manually
        // Runtime.getRuntime().availableProcessors(); // we can get the number of threads to set for number os StreamThreads.
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,"2");

        // this way setting up common timestamp extractor if we are streaming from multiple kafka topics
        // setting up a common timestamp extractor  is going to be problem
        //wiring the custom timestamp extractor into our kafka-stream app. this way we are configuring custom timestamp extractor.
        /**
         * when we made OrderTimeStampExtractor as default timestamp extractor then we got the error when store data is produced class-cast exception occurred
         *  ERROR o.apache.kafka.streams.KafkaStreams - stream-client [orders-app-d05d5747-571b-4bd2-ab1e-f1c3e32566b1] Encountered the following exception during processing and the registered exception handler opted to SHUTDOWN_CLIENT. The streams client is going to shut down now.
         * org.apache.kafka.streams.errors.StreamsException: Fatal user code error in TimestampExtractor callback for record ConsumerRecord(topic = stores, partition = 0, leaderEpoch = null, offset = 0, CreateTime = 1748859871759, serialized key size = 10, serialized value size = 161, headers = RecordHeaders(headers = [], isReadOnly = false), key = store_1234, value = Store[locationId=store_1234, address=Address[addressLine1=1234 Street 1 , addressLine2=, city=City1, state=State1, zip=12345], contactNum=1234567890]).
         * 	at org.apache.kafka.streams.processor.internals.RecordQueue.updateHead(RecordQueue.java:219)
         *
         * Caused by: java.lang.ClassCastException: class com.mylearning.orderkafkastreamsapp.domain.Store cannot be cast to class com.mylearning.orderkafkastreamsapp.domain.Order (com.mylearning.orderkafkastreamsapp.domain.Store and com.mylearning.orderkafkastreamsapp.domain.Order are in unnamed module of loader 'app')
         * 	at com.mylearning.orderkafkastreamsapp.util.OrderTimeStampExtractor.extract(OrderTimeStampExtractor.java:22)
         *
         */
        //config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimeStampExtractor.class);

        //config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

        createTopics(config, List.of(OrdersTopology.ORDERS,OrdersTopology.RESTAURANT_ORDERS,OrdersTopology.GENERAL_ORDERS,OrdersTopology.STORES));

        //Create an instance of KafkaStreamsr
        var kafkaStreams = new KafkaStreams(orderTopology, config);

        //This closes the streams anytime the JVM shuts down normally or abruptly.
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        try{
            kafkaStreams.start();
        }catch (Exception e ){
            log.error("Exception in starting the Streams : {}", e.getMessage(), e);
        }

    }

    private static void createTopics(Properties config, List<String> orders) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 1;
        short replication  = 1;

        // List<NewTopic>
        var newTopics = orders
                .stream()
                .map(topic ->{
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ",e.getMessage(), e);
        }
    }

}
