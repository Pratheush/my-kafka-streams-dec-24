package com.mylearning.greetingstreams.launcher;

import com.mylearning.greetingstreams.topology.GreetingsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;


/**
 *
 *  ### Command to produce messages in to the Kafka-Topic   ### below both of the commands for kafka-console-producer will work i.e. with only --broker-list or with only --bootstrap-server
 * .\bin\windows\kafka-console-producer.bat --broker-list  localhost:9092 --topic greetings
 *
 * .\bin\windows\kafka-console-producer.bat  --bootstrap-server localhost:9092 --topic greetings
 *
 *
 * #### Command to consume message from the Kafka-topic
 * .\bin\windows\kafka-console-consumer.bat  --bootstrap-server localhost:9092 --topic greetings-uppercase
 *
 *
 *
 *
 *
 * Publish to greetings topic with key and value
 * .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic greetings --property "key.separator=-" --property "parse.key=true"
 *
 * .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic greetings-spanish-uppercase --property "key.separator=-" --property "parse.key=true"
 *
 * Command to consume with Key
 * .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic greetings-uppercase --from-beginning -property "key.separator=-" --property "print.key=true"
 *
 * Here I have configured partition to 2 that means 2 tasks will be created behind the scenes
 *
 */
@Slf4j
public class GreetingsStreamApp {

    //private static Logger log = LoggerFactory.getLogger(GreetingsStreamApp.class);

    public static void main(String[] args) {

        // APPLICATION_ID_CONFIG is the Identifier of this application we can think of as Consumer-Group-IDs in Kafka-consumer
        // APPLICATION_ID_CONFIG is important this is how KafkaSteams Api is going to maintain its bookmark which means this is
        // how it knows what was the message that read in the Kafka-topic so when we restart the application it knows where to start reading from the kafka-topic
        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"greetings-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest"); // which means we are going to read the records that available in the kafka topic after the application spun up

        // setting up the number of StreamThreads manually
        // Runtime.getRuntime().availableProcessors(); // we can get the number of threads to set for number os StreamThreads.
        //properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,"2");

        //properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, GreetingStreamsDeserializationExceptionHandler.class);

        //properties.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamsSerializationExceptionHandler.class);

        // Another way of configuring Default Kafka Serdes Key and Value Serializer and Deserializer if we don't specifically specify at topology with Consumed.with() and Produced.with()
        // we can use this kind of configuration only when we are sure about that we are going to deal with only one particular key and value serializer and deserializer i.e. Default key and value Serde class
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        createTopics(properties,List.of(GreetingsTopology.GREETINGS,GreetingsTopology.GREETINGS_UPPERCASE,GreetingsTopology.GREETINGS_SPANISH,GreetingsTopology.GREETINGS_SPANISH_UPPERCASE));

        Topology greetingsTopology = GreetingsTopology.buildTopology();

        // KafkaStreams this is the class which takes care of starting our application basically this is the one going to execute our topology
        // this is the one actually going to execute our topology the pipeline which involves Source-Processor and Stream-Processing-Logic and Sink-Processor
        // KafkaStreams instance start-up the application and properties tells which kafka cluster its going to interact with
        KafkaStreams kafkaStreams=new KafkaStreams(greetingsTopology, properties);

        //kafkaStreams.setUncaughtExceptionHandler(new StreamProcessorCustomErrorHandler());

        // when this application shuts-down we need to make sure that releasing all the resources that are accessed by this KafkaStreams Application
        // so we can do a Shutdown-Hook anytime we shutdown this app this shutdown-hook is going to be invoked and it's going to call the close function of KafkaStreams
        //  this will do the graceful shutdown this will take care of releasing all the resources that was accessed by this Greetings-Application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        try{
            kafkaStreams.start();
        }catch (Exception e){
            log.error("Exception in the Starting the Stream : {}",e.getMessage(),e);
        }

    }

    // this function is going to create kafka-topic for us.
    // so using two Kafka-Topics 1 GREETINGS and 2 GREETINGS_UPPERCASE
    // here this is a programmatic way of creating Kafka-topics passing the topics-name as list and configs represents the broker details for this application
    private static void createTopics(Properties config, List<String> topicList) {

        AdminClient admin = AdminClient.create(config);
        int partitions = 2;
        short replication  = 1;

        // creating instance of NewTopic
        List<NewTopic> newTopics = topicList
                .stream()
                .map(topic -> {
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        CreateTopicsResult createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ",e.getMessage(), e);
        }
    }
}
