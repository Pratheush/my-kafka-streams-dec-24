package com.mylearning.greetingstreams.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.mylearning.greetingstreams.domain.Greeting;
import com.mylearning.greetingstreams.exception.GreetingRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

/**
 * we are building custom serdes for GreetingStream i.e. greetings message which is in json format which has message and timestamp both
 * {
 *     "message": "Hello",
 *     "timestamp": "2024-12-04T05:26:31.060293"
 * }
 *
 * while building custom serdes we implement Serializer i.e. from apache.kafka.common.serialization.Serializer of type Greeting.
 * we only need to implement serialize() and remaining methods are no op
 *
 * we are building custom serdes for Greeting for Serialization
 * similarly we have to build custom serdes for Greeting for Deserialization
 */

@Slf4j
public class GreetingSerializer implements Serializer<Greeting> {

    private final ObjectMapper objectMapper;

    public GreetingSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /*@Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }*/

    @Override
    public byte[] serialize(String topic, Greeting data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException GreetingSerializer::serialize : {}",e.getMessage(),e);
            throw new GreetingRuntimeException(e);
        } catch (Exception e){
            log.error("Exception GreetingSerializer :: serialize :: {}",e.getMessage(),e);
            throw new GreetingRuntimeException(e);
        }
    }

   /* @Override
    public byte[] serialize(String topic, Headers headers, Greeting data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }*/
}
