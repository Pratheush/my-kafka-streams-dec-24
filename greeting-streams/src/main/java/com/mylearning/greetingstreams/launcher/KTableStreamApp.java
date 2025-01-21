package com.mylearning.greetingstreams.launcher;

import com.mylearning.greetingstreams.topology.ExploreKTableTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;

import java.util.List;
import java.util.Properties;


@Slf4j
public class KTableStreamApp {

    public static void main(String[] args) {

        var kTableTopology = ExploreKTableTopology.buildTopology();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable"); // consumer group
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        createTopics(config, List.of(ExploreKTableTopology.KTABLE_WORDS));
        KafkaStreams kafkaStreams = new KafkaStreams(kTableTopology, config);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        log.info("Starting words-store streams");
        try {
            kafkaStreams.start();
        } catch (IllegalStateException e) {
            log.error("IllegalStateException Failed to start kafkaStreams {}", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (StreamsException e) {
            log.error(" StreamsException Failed to start kafkaStreams {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private static void createTopics(Properties config, List<String> greetings) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 1;
        short replication  = 1;

        List<NewTopic> newTopics = greetings
                .stream()
                .map(topic ->{
                    return new NewTopic(topic, partitions, replication);
                })
                .toList();

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
