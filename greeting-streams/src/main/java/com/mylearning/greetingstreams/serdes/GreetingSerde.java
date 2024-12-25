package com.mylearning.greetingstreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import com.mylearning.greetingstreams.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Building our custom GreetingSerde class implementing Serde<Greeting> overriding  the serializer and deserializer methods for our custom GreetingSerializer and GreetingDeserializer
 * which is in json format containing message and java time timestamp.
 */
@Slf4j
public class GreetingSerde implements Serde<Greeting> {

    // if we don't configure ObjectMapper with JavaTimeModule for timestamp like this then
    // we will see incorrect or inappropriate data that being produced and consumed from Kafka-Topic
    // this config is necessary inorder to make sure that all the dates get parsed successfully
    private final ObjectMapper objectMapper =new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,false);
    @Override
    public Serializer<Greeting> serializer() {
        return new GreetingSerializer(objectMapper);
    }

    @Override
    public Deserializer<Greeting> deserializer() {
        return new GreetingDeserializer(objectMapper);
    }
}
