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
 *
 *
 * -------------------------------
 *  USE RECOMMENDED : Use Parameterized Logging
 *  log.info("Processing user with ID: {}", userId);
 *
 *  Not Recommended:  This loses the stack trace unless you log ex and less efficient using string concatenation.
 *  log.error("Failed to process user with ID: " + userId + " due to " + ex.getMessage());
 *
 *  1. Logging Entry/Exit Points (Optional but Useful)
 *  log.info("Entering processOrder with orderId: {}", orderId);
 * // method logic
 * log.info("Exiting processOrder with status: {}", status);
 *
 * 2. With Conditions:
 * To avoid unnecessary log statement creation (especially for debug level), use:
 * if (log.isDebugEnabled()) {
 *     log.debug("Calculated value: {}", complexCalculation());
 * }
 *
 * 3. Pro Tip: Custom Log Prefixes
 * Add contextual prefixes to log messages to distinguish modules:
 * log.info("[OrderService] Saving order with ID: {}", orderId);
 * log.error("[PaymentService] Payment failed for transaction: {}", txnId, ex);
 *
 * 4. While Logging Error :
 *  log.error("Failed to process user with ID: {}", userId, ex);
 *  This prints the message and the full stack trace.
 *
 * 5. Logging Entry/Exit Points (Optional but Useful)
 *  log.info("Entering processOrder with orderId: {}", orderId);
 * // method logic
 * log.info("Exiting processOrder with status: {}", status);
 *
 *
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

            // RECOMMENDED WAY OF LOGGING : Add contextual prefixes to log messages to distinguish modules:
            log.error(" [GreetingSerializer] JsonProcessingException ::serialize : {}",data,e);
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
