package com.mylearning.orderkafkastreamsapp.topology;


import com.mylearning.orderkafkastreamsapp.domain.*;
import com.mylearning.orderkafkastreamsapp.serdes.OrderSerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.event.KeyValuePair;

import java.math.BigDecimal;

/**
 *Command to consume from Kafka-Topic
 * .\bin\windows\kafka-console-consumer.bat  --bootstrap-server localhost:9092 --topic orders-restaurant
 * .\bin\windows\kafka-console-consumer.bat  --bootstrap-server localhost:9092 --topic orders-general
 *
 * The primary purpose of a ValueMapper is to transform the values of records within a Kafka stream.
 * When processing a stream, you might need to modify or transform the values of the records according to your application's logic.
 * Kafka Streams offers various operations like mapValues(), flatMapValues(), transformValues(), etc., that accept a ValueMapper.
 * These operations apply the ValueMapper function to each record's value within the stream.
 * Example Scenario:
 * Let's consider an example where you have an input stream of Order objects and you want to transform each Order object into a Revenue object.
 * You would define a ValueMapper<Order, Revenue> where the input type is Order and the output type is Revenue.
 * Inside the ValueMapper, you would implement the logic to extract relevant information from the Order object and construct a Revenue object.
 *
 * public class OrderToRevenueMapper implements ValueMapper<Order, Revenue> {
 *     @Override
 *     public Revenue apply(Order order) {
 *         // Extract relevant information from the order and construct a revenue object
 *         double revenueAmount = order.getAmount() * order.getPrice();
 *         return new Revenue(order.getId(), revenueAmount);
 *     }
 * }
 *
 */
@Slf4j
public class OrdersTopology {

    public static final String ORDERS = "orders";
    public static final String RESTAURANT_ORDERS= "orders-restaurant";
    public static final String RESTAURANT_ORDERS_COUNT= "orders-restaurant-count";
    public static final String RESTAURANT_ORDERS_TOTAL_REVENUE= "orders-restaurant-total-revenue";
    public static final String GENERAL_ORDERS= "orders-general";
    public static final String GENERAL_ORDERS_COUNT= "orders-general-count";
    public static final String GENERAL_ORDERS_TOTAL_REVENUE= "orders-general-total-revenue";
    public static final String STORES = "stores";

    public static Topology buildTopology(){

        Predicate<? super String,? super Order> generalPredicate= (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<? super String,? super Order> restaurantPredicate= (key,order) -> order.orderType().equals(OrderType.RESTAURANT);

        StreamsBuilder streamsBuilder=new StreamsBuilder();

        /**
         *  selectKey() will re-key the records from orderId to locationId
         * .selectKey((key, value) -> value.locationId()) when we use this selectKey() we don't need to use map() or groupBy() to re-key the records therefore we can use groupByKey() and count()/aggregate()/reduce() operation to process.
         */
        KStream<String, Order> orderStream= streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), OrderSerdesFactory.orderSerde()))
                .selectKey((key, value) -> value.locationId()) // 14. Re-Keying Kafka Records for Stateful operations >> 2. Re-Keying using the selectKey operator
                ;

        orderStream.print(Printed.<String,Order>toSysOut().withLabel(ORDERS));

        /*KTable<String, Store> storeKTable=streamsBuilder
                    .table(STORES, Consumed.with(Serdes.String(),OrderSerdesFactory.storeSerde()),
                        Materialized.<String,Store,KeyValueStore<Bytes,byte[]>>as(STORES));

        storeKTable
                .toStream()
                        .print(Printed.<String,Store>toSysOut().withLabel(STORES));



        exploreOrderCount(orderStream, generalPredicate, GENERAL_ORDERS_COUNT);
        exploreOrderCount(orderStream, restaurantPredicate, RESTAURANT_ORDERS_COUNT);

        totalRevenue(orderStream,GENERAL_ORDERS_TOTAL_REVENUE,generalPredicate,storeKTable);
        totalRevenue(orderStream,RESTAURANT_ORDERS_TOTAL_REVENUE,restaurantPredicate, storeKTable);*/

        splitUsingBranched(orderStream, generalPredicate, restaurantPredicate);

        //mySplitUsingFilter(orderStream);


        return streamsBuilder.build();
    }

    //   tutorial way of doing the split of OrderStream into Two General and Restaurant and produce to two different kafka-topics
    private static void splitUsingBranched(KStream<String, Order> orderStream, Predicate<? super String, ? super Order> generalPredicate, Predicate<? super String, ? super Order> restaurantPredicate) {

        // ValueMapper<Value,NewMappedOutPutValue> ValueMapper maps Value to NewMappedOutPutValue here i.e. from Order to Revenue.
        ValueMapper<Order,Revenue> revenueValueMapper=order -> new Revenue(order.locationId(), order.finalAmount());

        /**
         * Split this stream into different branches. The returned BranchedKStream instance can be used for routing
         * the records to different branches depending on evaluation against the supplied predicates.
         *
         * BranchedKStream<K, V> branch(Predicate<? super K, ? super V> var1, Branched<K, V> var2);
         */
        orderStream
                .split(Named.as("Restaurant_General_Orders"))
                        .branch(generalPredicate,
                            Branched.withConsumer(generalOrderStream ->{

                            // this statement is for debugging purposes while we are developing application. this is not needed in production code.
                            generalOrderStream.print(Printed.<String,Order>toSysOut().withLabel("GENERAL-ORDER-STREAM"));

                                /**
                                 * transform the Order into Revenue and Publish The Transaction Amount to THe Topic using ValueMapper
                                 * mapValues() method : Transform the value of each input record into a new value (with possible new type) of the output record
                                 */
                            generalOrderStream
                                    .mapValues((readOnlyKey,order) -> revenueValueMapper.apply(order))
                            .to(GENERAL_ORDERS,Produced.with(Serdes.String(),OrderSerdesFactory.revenueSerde()));
                            //.to(GENERAL_ORDERS,Produced.with(Serdes.String(),OrderSerdesFactory.orderSerde()));

                                /**
                                 * 13. Aggregation in Order Management Application - A Real Time Use Case
                                 *  1. Total number of orders by each store using count operator
                                 */
                                aggregateOrdersByCount(generalOrderStream,GENERAL_ORDERS_COUNT);
                                /**
                                 * 13. Aggregation in Order Management Application - A Real Time Use Case
                                 *  2. Total Revenue made from the orders by each store using aggregate operator
                                 */
                                aggregateOrdersByRevenue(generalOrderStream, GENERAL_ORDERS_TOTAL_REVENUE);

                                }))
                        .branch(restaurantPredicate,
                                Branched.withConsumer(restaurantOrderStream ->{

                                            restaurantOrderStream.print(Printed.<String,Order>toSysOut().withLabel("RESTAURANT-ORDER-STREAM"));

                                            restaurantOrderStream
                                                    .mapValues((readOnlyKey,order) -> revenueValueMapper.apply(order))
                                                    .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(),OrderSerdesFactory.revenueSerde()));
                                                    //.to(RESTAURANT_ORDERS, Produced.with(Serdes.String(),OrderSerdesFactory.orderSerde()));

                                    /**
                                     * From Tutorial Section
                                     * 13. Aggregation in Order Management Application - A Real Time Use Case
                                     *  1. Total number of orders by each store using count operator
                                     */
                                    aggregateOrdersByCount(restaurantOrderStream,RESTAURANT_ORDERS_COUNT);
                                    /**
                                     * From Tutorial section
                                     * 13. Aggregation in Order Management Application - A Real Time Use Case
                                     *  2. Total Revenue made from the orders by each store using aggregate operator
                                     */
                                    aggregateOrdersByRevenue(restaurantOrderStream, RESTAURANT_ORDERS_TOTAL_REVENUE);

                                        })
                                        );
    }


    // my way of doing the split of OrderStream into Two General and Restaurant
    private static void mySplitUsingFilter(KStream<String, Order> orderStream) {

        KStream<String,Revenue> restaurantOrderStream= orderStream
                                .filter((k,v)->v.orderType().equals(OrderType.RESTAURANT))
                .mapValues((readOnlyKey,order) ->{
                    String locationId= order.locationId();
                    BigDecimal revenue=order.finalAmount();
                    return new Revenue(locationId,revenue);
                });

        restaurantOrderStream.print(Printed.<String,Revenue>toSysOut().withLabel("RESTAURANT-ORDER-STREAM"));


        KStream<String,Revenue> generalOrderStream= orderStream
                                .filter((k,v)->v.orderType().equals(OrderType.GENERAL))
                .mapValues((readOnlyKey,order) ->{
                    String locationId= order.locationId();
                    BigDecimal revenue=order.finalAmount();
                    return new Revenue(locationId,revenue);
                });

        generalOrderStream.print(Printed.<String,Revenue>toSysOut().withLabel("GENERAL-ORDER-STREAM"));


        restaurantOrderStream.to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), OrderSerdesFactory.revenueSerde()));

        generalOrderStream.to(GENERAL_ORDERS,Produced.with(Serdes.String(),OrderSerdesFactory.revenueSerde()));
    }

    private static void exploreOrderCount(KStream<String, Order> orderStream, Predicate<? super String,? super Order> predicateOrderType, String orderTypeCount){


        KTable<String,Long> ordersCount =
        orderStream
                .filter(predicateOrderType)
                .map((key, value) -> KeyValue.pair(value.locationId(),value)) // using map() to re-key the records since I am using selectKey() above
                .groupByKey(Grouped.with(Serdes.String(),OrderSerdesFactory.orderSerde()))
                .count(Named.as(orderTypeCount), Materialized.as(orderTypeCount));

        ordersCount
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel(orderTypeCount));

    }

    /**
     * 13. Aggregation in Order Management Application - A Real Time Use Case
     * 1. Total number of orders by each store using count operator
     * @param orderKStream
     * @param storeName
     */

    private static void aggregateOrdersByCount(KStream<String, Order> orderKStream, String storeName) {
        KeyValueMapper<String,Order,KeyValue<String,Order>> locationIdKeyValueMapper = (key,value) -> KeyValue.pair(value.locationId(),value);

        /**
         * using map() to re-key the records, since .selectKey((key, value) -> value.locationId()) is not used above for transforming key from orderId to locationId
         * all map() operation works when we have to re-key the record from orderId to locationId then we can group the records based on key by using groupByKey()
         * or
         * we can use groupBy() operation directly without using map() operation to re-key the records from orderId to locationId and group the records based on key.
         *
         * Materialized View creates changelog internal partition i.e. Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("ORDER-COUNT"+storeName)
         *
         * Repartition is the topic which comes into play when we are changing the key for any record here in this case we are not using orderId instead we are adding a new key i.e. locationId
         * in those kind of scenarios data's get return back to the repartition topic and then the whole process of reconstructing this KeyValue-Pair happens behind the scenes.
         * so all the kafka-records return into the repartition topic and then read from the repartition topic so that it represents the latest value.
         */
        KTable<String, Long> ordersCount = orderKStream
                .peek((key, orderValue) -> log.info("Key : {}, OrderValue : {}", key, orderValue))
                //.map(locationIdKeyValueMapper)
                //.map(locationIdKeyValueMapper::apply)
                //.map(((key, value) -> KeyValue.pair(value.locationId(), value)))  // using map() to re-key the records, since .selectKey((key, value) -> value.locationId()) is not used to transforming key from orderId to locationId
                .groupByKey(Grouped.with(Serdes.String(), OrderSerdesFactory.orderSerde()))
                //.groupBy((key, value) -> value.locationId(),Grouped.with(Serdes.String(), OrderSerdesFactory.orderSerde()))  // we can also use groupBy() instead of groupByKey() we use groupBy() when we need to decide the Key of different type and, here is locationId and order is key and value.
                .count(Named.as("ORDER-COUNT"+storeName), Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("ORDER-COUNT"+storeName));

        ordersCount
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel("ORDER-TYPE-COUNT"));
    }

    /**
     * 13. Aggregation in Order Management Application - A Real Time Use Case
     * 2. Total Revenue made from the orders by each store using aggregate operator
     */

    private static void aggregateOrdersByRevenue(KStream<String, Order> orderStream, String storeName) {
        KeyValueMapper<String,Order,KeyValue<String,Order>> locationIdKeyValueMapper = (key,value) -> KeyValue.pair(value.locationId(),value);

        Initializer<TotalRevenue> totalRevenueInitializer= TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> totalRevenueAggregator=(key, value, totalRevenue) -> totalRevenue.updateTotalRevenue(key, value);

        /**
         * using map() to re-key the records, since .selectKey((key, value) -> value.locationId()) is not used above for transforming key from orderId to locationId
         * all map() operation works when we have to re-key the record from orderId to locationId then we can group the records based on key by using groupByKey()
         * or
         * we can use groupBy() operation directly without using map() operation to re-key the records from orderId to locationId and group the records based on key.
         *
         * Materialized View creates changelog internal partition i.e. Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("ORDER-COUNT"+storeName)
         *
         * Repartition is the topic which comes into play when we are changing the key for any record here in this case we are not using orderId instead we are adding a new key i.e. locationId
         * in those kind of scenarios data's get return back to the repartition topic and then the whole process of reconstructing this KeyValue-Pair happens behind the scenes.
         * so all the kafka-records return into the repartition topic and then perform the repartition and reading from the repartition topic so that it represents the latest value.
         */
        KTable<String, TotalRevenue> aggregatedTotalRevenue = orderStream
                .peek(((key, value) -> log.info("KEY :: {}, ORDER-VALUE : {}", key, value)))
                //.map(locationIdKeyValueMapper)
                //.map(locationIdKeyValueMapper::apply)
                //.map(((key, value) -> KeyValue.pair(value.locationId(),value)))
                .groupByKey(Grouped.with(Serdes.String(), OrderSerdesFactory.orderSerde()))
                //.groupBy(((key, value) -> value.locationId()), (Grouped.with(Serdes.String(), OrderSerdesFactory.orderSerde())))
                .aggregate(
                        totalRevenueInitializer,
                        totalRevenueAggregator,
                        Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(OrderSerdesFactory.totalRevenueSerde())
                );

        aggregatedTotalRevenue
                .toStream()
                .print(Printed.<String, TotalRevenue>toSysOut().withLabel(storeName.toUpperCase()));
    }

    /**
     * 13. Aggregation in Order Management Application - A Real Time Use Case
     * 2. Total Revenue made from the orders by each store using aggregate operator
     * @param orderKStream
     * @param orderTotalRevenue
     * @param predicateOrderType
     * @param storeKtable
     */
    private static void totalRevenue(KStream<String, Order> orderKStream, String orderTotalRevenue, Predicate<? super String,? super Order> predicateOrderType, KTable<String,Store> storeKtable){

        Initializer<TotalRevenue> totalRevenueInitializer= TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> totalRevenueAggregator=(key, value, totalRevenue) -> totalRevenue.updateTotalRevenue(key, value);

        KTable<String,TotalRevenue> aggregatedTotalRevenue=orderKStream
                .filter(predicateOrderType)
                //.map((key, value) -> KeyValue.pair(value.locationId(),value) )        // using map() to re-key the records since I am using selectKey() above
                .groupByKey(Grouped.with(Serdes.String(),OrderSerdesFactory.orderSerde()))
                .aggregate(
                        totalRevenueInitializer,
                        totalRevenueAggregator,
                        Materialized.<String, TotalRevenue,KeyValueStore<Bytes,byte[]>>as(orderTotalRevenue)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(OrderSerdesFactory.totalRevenueSerde())
                );

        aggregatedTotalRevenue.toStream()
                .print(Printed.<String,TotalRevenue>toSysOut().withLabel(orderTotalRevenue));


        totalRevenueWithADDress(aggregatedTotalRevenue,storeKtable);

    }

    // Now we have new business requirement we have to enrich the data with the store information
    // like store address and store contact-number with the TotalRevenue of each store in a location
    // Store information is not running data i.e. store information does not change often unless their phone number is changed or something like that
    // so that's why the Store information will be held on KTable whereas TotalRevenue is continuously changing with
    // each order places order number and total revenue from a store will change
    // therefore TotalRevenue will be held on KStream
    // KTable-KTable
    private static void totalRevenueWithADDress(KTable<String,TotalRevenue> totalRevenueKTable, KTable<String, Store> storeKTable){

        ValueJoiner<TotalRevenue,Store,TotalRevenueWithAddress> totalRevenueAddressValueJoiner= TotalRevenueWithAddress::new;

        KTable<String,TotalRevenueWithAddress> joinedKtable= totalRevenueKTable.join(storeKTable, totalRevenueAddressValueJoiner);

        joinedKtable
                .toStream()
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel("TOTAL-REVENUE-WITH-ADDRESS"));


    }


}
