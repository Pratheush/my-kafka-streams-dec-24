package com.mylearning.greetingstreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.mylearning.greetingstreams.domain.Greeting;
import com.mylearning.greetingstreams.exception.GreetingRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

@Slf4j
public class GreetingDeserializer implements Deserializer<Greeting> {

    private final ObjectMapper objectMapper;

    public GreetingDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public Greeting deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Greeting.class);
        } catch (IOException e) {
            log.error("IOException :: GreetingDeserializer.deserialize :: {}",e.getMessage(),e);
            throw new GreetingRuntimeException(e);
        } catch (Exception e){
            log.error("Exception :: GreetingDeserializer.deserial :: {}",e.getMessage(),e);
            throw new GreetingRuntimeException(e);
        }
    }

    @Override
    public Greeting deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public Greeting deserialize(String topic, Headers headers, ByteBuffer data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
