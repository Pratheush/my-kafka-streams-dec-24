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

        KStream<String, Order> orderStream= streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), OrderSerdesFactory.orderSerde()))
                .selectKey((key, value) -> value.locationId());

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

                                }))
                        .branch(restaurantPredicate,
                                Branched.withConsumer(restaurantOrderStream ->{

                                            restaurantOrderStream.print(Printed.<String,Order>toSysOut().withLabel("RESTAURANT-ORDER-STREAM"));

                                            restaurantOrderStream
                                                    .mapValues((readOnlyKey,order) -> revenueValueMapper.apply(order))
                                                    .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(),OrderSerdesFactory.revenueSerde()));
                                                    //.to(RESTAURANT_ORDERS, Produced.with(Serdes.String(),OrderSerdesFactory.orderSerde()));
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
                //.map((key, value) -> KeyValue.pair(value.locationId(),value)) // using map() to re-key the records since I am using selectKey() above
                .groupByKey(Grouped.with(Serdes.String(),OrderSerdesFactory.orderSerde()))
                .count(Named.as(orderTypeCount), Materialized.as(orderTypeCount));

        ordersCount
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel(orderTypeCount));

    }

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
