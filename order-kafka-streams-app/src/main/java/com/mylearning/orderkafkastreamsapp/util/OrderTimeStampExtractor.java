package com.mylearning.orderkafkastreamsapp.util;

import com.mylearning.orderkafkastreamsapp.domain.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * here in this custom timestamp extractor we are going to get the actual record and from the record i.e. order
 * we get the orderedDateTime, and then we are going to convert that to a instant and return long value.
 *
 * all the windows will be created based on the extracted value from orderedDateTime
 * and time-windows are created and records will be aggregated accordingly
 */
@Slf4j
public class OrderTimeStampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> orderRecord, long partitionTime) {
        var order = (Order) orderRecord.value();
        // now doing null checks
        if( order!=null && order.orderedDateTime()!=null){
            LocalDateTime timestamp = order.orderedDateTime();
            log.info("OrderTimeStampExtractor timestamp in extractor : {}", timestamp);
            
            return convertToInstantFromIST(timestamp);
            
        }
        // let's say if orderedDateTime is null then returning partitionTime
        // but ideally if orderedDateTime is null then we should throw an exception
        return partitionTime;
    }

    // toEpochMilli() when we call this it's going to return long value
    private long convertToInstantFromIST(LocalDateTime timestamp) {
        return timestamp.toInstant(ZoneOffset.ofHoursMinutes(+5,+30)).toEpochMilli();
    }

    // in case if we are already getting GMT time then
    // converting locatlDateTime which is in UTC to actual long value that represents UTC timestamp
    private long convertToInstantFromUTC(LocalDateTime timestamp) {
        return timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
