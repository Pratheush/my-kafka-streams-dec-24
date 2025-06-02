package com.mylearning.orderkafkastreamsapp.topology;


import com.mylearning.orderkafkastreamsapp.domain.*;
import com.mylearning.orderkafkastreamsapp.serdes.OrderSerdesFactory;
import com.mylearning.orderkafkastreamsapp.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.event.KeyValuePair;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

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
 *
 * ValueMapper: Used to map a value to a new value without modifying the key. It is a stateless operation applied to each record individually.
 *
 * ValueMapperWithKey: Similar to ValueMapper, but it also has access to the key while transforming the value.
 * This allows for transformations that depend on both the key and value.
 *
 * KeyValueMapper: Used when both the key and value need to be transformed. This is useful when you want to modify the key along with the value
 *
 * The ValueJoiner interface for joining two values into a new value of arbitrary type. This is a stateless operation
 *
 */
@Slf4j
public class OrdersTopology {

    public static final String ORDERS = "orders";
    public static final String RESTAURANT_ORDERS= "orders-restaurant";
    public static final String RESTAURANT_ORDERS_COUNT= "orders-restaurant-count";
    public static final String RESTAURANT_ORDERS_TOTAL_REVENUE= "orders-restaurant-total-revenue";
    public static final String RESTAURANT_ORDERS_COUNT_WINDOW = "orders-restaurant-count-window";
    public static final String RESTAURANT_ORDERS_REVENUE_WINDOW = "orders-restaurant-revenue-window";
    public static final String GENERAL_ORDERS= "orders-general";
    public static final String GENERAL_ORDERS_COUNT= "orders-general-count";
    public static final String GENERAL_ORDERS_TOTAL_REVENUE= "orders-general-total-revenue";
    public static final String GENERAL_ORDERS_COUNT_WINDOW = "orders-general-count-window";
    public static final String GENERAL_ORDERS_REVENUE_WINDOW = "orders-general-revenue-window";
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
                .stream(ORDERS, Consumed.with(Serdes.String(), OrderSerdesFactory.orderSerde())
                        .withTimestampExtractor(new OrderTimeStampExtractor()) // setting up specific timestamp extractor to each instance of stream
                        )
                .selectKey((key, value) -> value.locationId()) // 14. Re-Keying Kafka Records for Stateful operations >> 2. Re-Keying using the selectKey operator
                ;

        /**
         * [orders]: store_1234, Order[orderId=12345, locationId=store_1234, finalAmount=27.00, orderType=GENERAL, orderLineItems=[OrderLineItem[item=Bananas, count=2, amount=2.00], OrderLineItem[item=Iphone Charger, count=1, amount=25.00]], orderedDateTime=2025-03-07T17:14:57.921089900]
         * [orders]: store_1234, Order[orderId=54321, locationId=store_1234, finalAmount=15.00, orderType=RESTAURANT, orderLineItems=[OrderLineItem[item=Pizza, count=2, amount=12.00], OrderLineItem[item=Coffee, count=1, amount=3.00]], orderedDateTime=2025-03-07T17:14:57.921089900]
         * [orders]: store_4567, Order[orderId=12345, locationId=store_4567, finalAmount=27.00, orderType=GENERAL, orderLineItems=[OrderLineItem[item=Bananas, count=2, amount=2.00], OrderLineItem[item=Iphone Charger, count=1, amount=25.00]], orderedDateTime=2025-03-07T17:14:57.921089900]
         * [orders]: store_4567, Order[orderId=12345, locationId=store_4567, finalAmount=27.00, orderType=RESTAURANT, orderLineItems=[OrderLineItem[item=Bananas, count=2, amount=2.00], OrderLineItem[item=Iphone Charger, count=1, amount=25.00]], orderedDateTime=2025-03-07T17:14:57.922089]
         */
        // Generics are applied before toSysOut() because Printed.<String, Order> is specifying the type parameters for the Printed class or method before calling toSysOut().
        orderStream.print(Printed.<String,Order>toSysOut().withLabel(ORDERS));

        /**
         * [stores]: store_1234, Store[locationId=store_1234, address=Address[addressLine1=1234 Street 1 , addressLine2=, city=City1, state=State1, zip=12345], contactNum=1234567890]
         * [stores]: store_4567, Store[locationId=store_4567, address=Address[addressLine1=1234 Street 2 , addressLine2=, city=City2, state=State2, zip=541321], contactNum=0987654321]
         */
        KTable<String, Store> storeKTable=streamsBuilder
                    .table(STORES, Consumed.with(Serdes.String(),OrderSerdesFactory.storeSerde())
                        ,Materialized.<String,Store,KeyValueStore<Bytes,byte[]>>as(STORES));

        storeKTable
                .toStream()
                        .print(Printed.<String,Store>toSysOut().withLabel(STORES));



        //exploreOrderCount(orderStream, generalPredicate, GENERAL_ORDERS_COUNT);
        //exploreOrderCount(orderStream, restaurantPredicate, RESTAURANT_ORDERS_COUNT);

        //totalRevenue(orderStream,GENERAL_ORDERS_TOTAL_REVENUE,generalPredicate,storeKTable);
        //totalRevenue(orderStream,RESTAURANT_ORDERS_TOTAL_REVENUE,restaurantPredicate, storeKTable);



        splitUsingBranched(orderStream, generalPredicate, restaurantPredicate,storeKTable);

        //mySplitUsingFilter(orderStream);


        return streamsBuilder.build();
    }

    // Predicate<? super String, ? super Order> generalPredicate, Predicate<? super String, ? super Order> restaurantPredicate
    //   tutorial way of doing the split of OrderStream into Two General and Restaurant and produce to two different kafka-topics
    private static void splitUsingBranched(KStream<String, Order> orderStream, Predicate<? super String, ? super Order> generalPredicate, Predicate<? super String, ? super Order> restaurantPredicate, KTable<String,Store> storeKTable) {

        // ValueMapper<InputValue,NewMappedOutPutValue> ValueMapper maps Value to NewMappedOutPutValue here i.e. from Order to Revenue.
        ValueMapper<Order,Revenue> revenueValueMapper=order -> new Revenue(order.locationId(), order.finalAmount());
        ValueMapperWithKey<? super String,? super Order, ? extends Revenue> revenueValueMapperWithKey = (stringKey,orderValue) -> new Revenue(orderValue.locationId(),orderValue.finalAmount());

        /**
         * Split this stream into different branches. The returned BranchedKStream instance can be used for routing
         * the records to different branches depending on evaluation against the supplied predicates.
         *
         * this split() allow us to apply some kind of branching strategy.
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
                                    //.mapValues(revenueValueMapperWithKey)
                                    .mapValues((readOnlyKey,order) -> revenueValueMapper.apply(order))
                            //.to(GENERAL_ORDERS);
                            .to(GENERAL_ORDERS,Produced.with(Serdes.String(),OrderSerdesFactory.revenueSerde()));
                            //.to(GENERAL_ORDERS,Produced.with(Serdes.String(),OrderSerdesFactory.orderSerde()));

                                /**
                                 * 13. Aggregation in Order Management Application - A Real Time Use Case
                                 *  1. Total number of orders by each store using count operator
                                 */
                                //aggregateOrdersByCount(generalOrderStream,GENERAL_ORDERS_COUNT);
                                /**
                                 * 13. Aggregation in Order Management Application - A Real Time Use Case
                                 *  2. Total Revenue made from the orders by each store using aggregate operator
                                 */
                                //aggregateOrdersByRevenue(generalOrderStream, GENERAL_ORDERS_TOTAL_REVENUE,storeKTable);

                                /**
                                 * 18. Widowing in Order Management Application - A Real Time Use Case
                                 *  3. Aggregate Number of Orders by Windows
                                 */
                                //aggregateOrdersCountByTimeWindow(generalOrderStream, GENERAL_ORDERS_COUNT_WINDOW);

                                /**
                                 * 18. Widowing in Order Management Application - A Real Time Use Case
                                 * 4. Aggregate Revenue by Windows
                                 */
                                aggregateOrdersRevenueByTimeWindow(generalOrderStream,GENERAL_ORDERS_REVENUE_WINDOW,storeKTable);

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
                                    //aggregateOrdersByCount(restaurantOrderStream,RESTAURANT_ORDERS_COUNT);
                                    /**
                                     * From Tutorial section
                                     * 13. Aggregation in Order Management Application - A Real Time Use Case
                                     *  2. Total Revenue made from the orders by each store using aggregate operator
                                     */
                                    //aggregateOrdersByRevenue(restaurantOrderStream, RESTAURANT_ORDERS_TOTAL_REVENUE,storeKTable);

                                    /**
                                     * 18. Widowing in Order Management Application - A Real Time Use Case
                                     *  3. Aggregate Number of Orders by Windows
                                     */
                                    //aggregateOrdersCountByTimeWindow(restaurantOrderStream, RESTAURANT_ORDERS_COUNT_WINDOW);

                                    /**
                                     * 18. Widowing in Order Management Application - A Real Time Use Case
                                     * 4. Aggregate Revenue by Windows
                                     */
                                    aggregateOrdersRevenueByTimeWindow(restaurantOrderStream,RESTAURANT_ORDERS_REVENUE_WINDOW,storeKTable);

                                        })
                                        );
    }


    // my way of doing the split of OrderStream into Two General and Restaurant
    private static void mySplitUsingFilter(KStream<String, Order> orderStream) {

        KStream<String,Revenue> restaurantOrderStream= orderStream
                                .filter((k,v)-> v.orderType().equals(OrderType.RESTAURANT))
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
     *  Aggregate the Orders Count per LocationId (so we need to re-key the kafka-record from OrderId to LocationId after streaming from Source Topic.)
     * @param orderKStream
     * @param storeName
     */

    private static void aggregateOrdersByCount(KStream<String, Order> orderKStream, String storeName) {
        /*
         * KeyValueMapper is used to transform key and value both.
         */
        KeyValueMapper<String,Order,KeyValue<String,Order>> locationIdKeyValueMapper = (key,value) -> KeyValue.pair(value.locationId(),value);

        /**
         * using map() to re-key the records, since .selectKey((key, value) -> value.locationId()) is not used above for transforming key from orderId to locationId
         * all map() operation works when we have to re-key the record from orderId to locationId then we can group the records based on key by using groupByKey()
         * or
         * we can use groupBy() operation directly without using map() operation to re-key the records from orderId to locationId and group the records based on key.
         *
         * Materialized View creates changelog internal partition i.e. Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("ORDER-COUNT"+storeName)
         *
         * Repartition is the topic which comes into play when we are changing the key for any record here in this case we are not using orderId instead we are
         * adding a new key i.e. locationId in those kind of scenarios data's get return back to the repartition topic and then the whole process of reconstructing
         * this KeyValue-Pair happens behind the scenes. so all the kafka-records return into the repartition topic and then read from the repartition topic so
         * that it represents the latest value.
         *
         * repartition is the concept where in distributed environment repartition is going to take care of placing the record in the appropriate partitions so any time we
         * change the key in this case we are using groupBy() operation then we have two internal topics are created one is changelog and second is repartition.
         */
        KTable<String, Long> ordersCount = orderKStream
                .peek((key, orderValue) -> log.info("Key : {}, OrderValue : {}", key, orderValue))
                //.map(locationIdKeyValueMapper)
                //.map(locationIdKeyValueMapper::apply)
                //.map(((key, value) -> KeyValue.pair(value.locationId(), value)))  // using map() to re-key the records, since .selectKey((key, value) -> value.locationId()) is not used to transforming key from orderId to locationId
                .groupByKey(Grouped.with(Serdes.String(), OrderSerdesFactory.orderSerde()))
                //.groupBy((key, value) -> value.locationId(),Grouped.with(Serdes.String(), OrderSerdesFactory.orderSerde()))  // we can also use groupBy() instead of groupByKey() we use groupBy() when we need to decide the Key of different type and, here locationId is key and Order is value.
                .count(Named.as("ORDER-COUNT"+storeName), Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("ORDER-COUNT"+storeName));

        ordersCount
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel("ORDER-TYPE-COUNT"));
    }

    /**
     * 13. Aggregation in Order Management Application - A Real Time Use Case
     * 2. Total Revenue made from the orders by each store using aggregate operator
     * Aggregate the Total Revenue from the orders by each store as per locationId here locationId is key. so we re-key the kafka record
     */

    private static void aggregateOrdersByRevenue(KStream<String, Order> orderStream, String storeName,KTable<String,Store> storeKTable) {
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
         * Repartition is the topic which comes into play when we are changing the key for any record here in this case we are not using orderId instead we are adding a
         * new key i.e. locationId in those kind of scenarios data's get return back to the repartition topic and then the whole process of reconstructing
         * this KeyValue-Pair happens behind the scenes. so all the kafka-records return into the repartition topic and then perform the repartition
         * and reading from the repartition topic so that it represents the latest value.
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

        totalRevenueWithADDress(aggregatedTotalRevenue,storeKTable);
    }

    /**
     * 13. Aggregation in Order Management Application - A Real Time Use Case
     * 2. Total Revenue made from the orders by each store using aggregate operator
     * @param orderKStream  key is order_id and value as Order
     * @param orderTotalRevenue     topic-name as well as store-name
     * @param predicateOrderType
     * @param storeKtable where  store-number as key and value as complete Store details
     *
     *  Materialized.<K,V,S>.as("State-Store-Name").withKeySerder().withValueSerde();
     *  <K> – type of record key <V> – type of record value <S> – type of state store (note: state stores always have key/ value types <Bytes,byte[]>
     *
     *   [orders-general-total-revenue]: store_1234, TotalRevenue[locationId=store_1234, runningOrderCount=1, runningRevenue=27.00]
     *   [orders-general-total-revenue]: store_4567, TotalRevenue[locationId=store_4567, runningOrderCount=1, runningRevenue=27.00]
     *   [orders-restaurant-total-revenue]: store_1234, TotalRevenue[locationId=store_1234, runningOrderCount=1, runningRevenue=15.00]
     *   [orders-restaurant-total-revenue]: store_4567, TotalRevenue[locationId=store_4567, runningOrderCount=1, runningRevenue=27.00]
     *
     *   aggregateOrdersByRevenue() this function has same business logic as totalRevenue() function has the only difference is that the way it implemented and another
     *   difference is in totalRevenue() function filter() operator is used on KStream.
     *
     *
     */
    private static void totalRevenue(KStream<String, Order> orderKStream, String orderTotalRevenue, Predicate<? super String,? super Order> predicateOrderType, KTable<String,Store> storeKtable){

        Initializer<TotalRevenue> totalRevenueInitializer= TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> totalRevenueAggregator=(key, value, totalRevenue) -> totalRevenue.updateTotalRevenue(key, value);

        KTable<String,TotalRevenue> aggregatedTotalRevenue=orderKStream
                .filter(predicateOrderType)
                //.map((key, value) -> KeyValue.pair(value.locationId(),value) )        // using map() to re-key the records since I am using selectKey() above so i am commenting this statement. re-key to locationId from orderId.
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

    /**
     *     Now we have new business requirement we have to enrich the data with the store information
     *      like store address and store contact-number with the TotalRevenue of each store in a location
     *      Store information is not running data i.e. store information does not change often unless their phone number is changed or something like that
     *      so that's why the Store information will be held on KTable whereas TotalRevenue is continuously changing with
     *      each order places order number and total revenue from a store will change
     *      therefore TotalRevenue will be held on KStream
     *      KTable-KTable
     *
     *       Here Joining Type KStream-KTable  (KTable : Store, KStream : TotalRevenue) but using KTable for both. because from above method splitUsingBranched() takes
     *       KStream and KTable and inside aggregateOrdersByRevenue() after aggregation operation result is KTable so we pass KTable-KTable join here in the
     *       method totalRevenueWithADDress().
     *
     *      16. Join in Order Management Application - A Real Time Use Case
     *
     *      OUTPUT in CONSOLE ::
     *      [TOTAL-REVENUE-WITH-ADDRESS]: store_1234, TotalRevenueWithAddress[totalRevenue=TotalRevenue[locationId=store_1234, runningOrderCount=1, runningRevenue=27.00], store=Store[locationId=store_1234, address=Address[addressLine1=1234 Street 1 , addressLine2=, city=City1, state=State1, zip=12345], contactNum=1234567890]]
     *      [TOTAL-REVENUE-WITH-ADDRESS]: store_4567, TotalRevenueWithAddress[totalRevenue=TotalRevenue[locationId=store_4567, runningOrderCount=1, runningRevenue=27.00], store=Store[locationId=store_4567, address=Address[addressLine1=1234 Street 2 , addressLine2=, city=City2, state=State2, zip=541321], contactNum=0987654321]]
     *      [TOTAL-REVENUE-WITH-ADDRESS]: store_1234, TotalRevenueWithAddress[totalRevenue=TotalRevenue[locationId=store_1234, runningOrderCount=1, runningRevenue=15.00], store=Store[locationId=store_1234, address=Address[addressLine1=1234 Street 1 , addressLine2=, city=City1, state=State1, zip=12345], contactNum=1234567890]]
     *      [TOTAL-REVENUE-WITH-ADDRESS]: store_4567, TotalRevenueWithAddress[totalRevenue=TotalRevenue[locationId=store_4567, runningOrderCount=1, runningRevenue=27.00], store=Store[locationId=store_4567, address=Address[addressLine1=1234 Street 2 , addressLine2=, city=City2, state=State2, zip=541321], contactNum=0987654321]]
     *
     *      The ValueJoiner interface for joining two values into a new value of arbitrary type. This is a stateless operation
     */
    private static void totalRevenueWithADDress(KTable<String,TotalRevenue> totalRevenueKTable, KTable<String, Store> storeKTable){

        ValueJoiner<TotalRevenue,Store,TotalRevenueWithAddress> totalRevenueAddressValueJoiner= TotalRevenueWithAddress::new;

        KTable<String,TotalRevenueWithAddress> joinedKtable= totalRevenueKTable.join(storeKTable, totalRevenueAddressValueJoiner);

        joinedKtable
                .toStream()
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel("TOTAL-REVENUE-WITH-ADDRESS"));


    }

    /**
     *
     * 18. Widowing in Order Management Application - A Real Time Use Case
     *  3. Aggregate Number of Orders by Windows
     *   we are creating TimeWindows and performing aggregation of orders(number of orders) by grouping them in time window
     * @param orderKStream
     * @param storeName
     * @return since there is void in return type so we don't have to mention @return
     */
    private static void aggregateOrdersCountByTimeWindow(KStream<String,Order> orderKStream,String storeName){

        /*
         * KeyValueMapper is used to transform key and value both.
         */
        KeyValueMapper<String,Order,KeyValue<String,Order>> locationIdKeyValueMapper = (key,value) -> KeyValue.pair(value.locationId(),value);

        Duration fifteenSecondWindow = Duration.ofSeconds(15);
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(fifteenSecondWindow);

        KTable<Windowed<String>, Long> ordersCount = orderKStream
                .peek((key, orderValue) -> log.info("Key : {}, OrderValue : {}", key, orderValue))
                //.map(locationIdKeyValueMapper)
                //.map(locationIdKeyValueMapper::apply)
                //.map(((key, value) -> KeyValue.pair(value.locationId(), value)))  // using map() to re-key the records, since .selectKey((key, value) -> value.locationId()) is not used to transforming key from orderId to locationId
                .groupByKey(Grouped.with(Serdes.String(), OrderSerdesFactory.orderSerde()))
                //.groupBy((key, value) -> value.locationId(),Grouped.with(Serdes.String(), OrderSerdesFactory.orderSerde()))  // we can also use groupBy() instead of groupByKey() we use groupBy() when we need to decide the Key of different type and, here locationId is key and Order is value.
                .windowedBy(tumblingWindow)
                .count(Named.as("ORDER-COUNT"+storeName), Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("ORDER-COUNT"+storeName))
                        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull())); // suppress() operator suppressing the results until the time window of 15 seconds exhausted.

        ordersCount
                .toStream()
                .peek(((key, value) -> {
                    log.info("StoreName : {}, key :: {}, value :: {}",storeName,key,value);
                    printLocalDateTimes(key,value);
                }))
                .print(Printed.<Windowed<String>,Long>toSysOut().withLabel("ORDER-TYPE-COUNT "+storeName));
    }

    /**
     * Aggregate the revenue of the orders by grouping them in time windows.
     * 18. Widowing in Order Management Application - A Real Time Use Case
     * 4. Aggregate Revenue by Windows
     * we are creating TimeWindows and performing aggregation of orders(number of orders) by grouping them in time window
     * @param orderKStream
     * @param storeName
     */
    private static void aggregateOrdersRevenueByTimeWindow(KStream<String,Order> orderKStream, String storeName,KTable<String,Store> storeKTable){

        Duration fifteenSecondWindow = Duration.ofSeconds(15);
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(fifteenSecondWindow);

        KeyValueMapper<String,Order,KeyValue<String,Order>> locationIdKeyValueMapper = (key,value) -> KeyValue.pair(value.locationId(),value);

        Initializer<TotalRevenue> totalRevenueInitializer= TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> totalRevenueAggregator=(key, value, totalRevenue) -> totalRevenue.updateTotalRevenue(key, value);

        KTable<Windowed<String>, TotalRevenue> aggregatedTotalRevenue = orderKStream
                .peek(((key, value) -> log.info("KEY :: {}, ORDER-VALUE : {}", key, value)))
                //.map(locationIdKeyValueMapper)
                //.map(locationIdKeyValueMapper::apply)
                //.map(((key, value) -> KeyValue.pair(value.locationId(),value)))
                .groupByKey(Grouped.with(Serdes.String(), OrderSerdesFactory.orderSerde()))
                //.groupBy(((key, value) -> value.locationId()), (Grouped.with(Serdes.String(), OrderSerdesFactory.orderSerde())))
                .windowedBy(tumblingWindow)
                .aggregate(
                        totalRevenueInitializer,
                        totalRevenueAggregator,
                        Materialized.<String, TotalRevenue, WindowStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(OrderSerdesFactory.totalRevenueSerde())
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull())); // suppress() operator suppressing the results until the time window of 15 seconds exhausted.


        aggregatedTotalRevenue
                .toStream()
                .peek(((key, value) -> {
                    log.info("StoreName : {}, key :: {}, value :: {}", storeName, key, value);
                    printLocalDateTimes(key, value);
                }))
                .print(Printed.<Windowed<String>, TotalRevenue>toSysOut().withLabel(storeName));

        ValueJoiner<TotalRevenue,Store,TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        // in this case for join we are performing here its recommended to provide JoinedParams otherwise we might run into runtime issues.
        // is defining how the keys and values should be serialized and deserialized during the join operation.
        /**
         * In Kafka Streams, the Joined parameter is used when performing join operations between two KStream, KTable, or GlobalKTable instances.
         *
         * The first parameter Serdes.String() specifies the key serde (String type).
         * The second parameter OrderSerdesFactory.totalRevenueSerde() defines the value serde for the left stream/table (TotalRevenue).
         * The third parameter OrderSerdesFactory.storeSerde() defines the value serde for the right stream/table (Store).
         */
        Joined<String, TotalRevenue, Store> joinedParams = Joined.with(Serdes.String(), OrderSerdesFactory.totalRevenueSerde(), OrderSerdesFactory.storeSerde());

       aggregatedTotalRevenue
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .join(storeKTable,valueJoiner,joinedParams)
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storeName +"-BY-TIME-WINDOW"));

    }

    // each window has startTime and endTime
    private static void printLocalDateTimes(Windowed<String> key, Object value) {
        Instant startTime = key.window().startTime(); // startTime type is Instant
        Instant endTime = key.window().endTime();   // any time windows are created it is going to be in gmt time not the localTime
        log.info("startTime : {}, endTime : {}, VALUE : {}", startTime, endTime, value); // here printing the instant startTime and endTime  :: windowed key startTime and endTime are in gmt format

        // converting the instant value into local timestamp using zone since i am in IST i.e entry("IST", "Asia/Kolkata"),. we can get the code by clicking into SHORT_IDS
        // getting the localtime from the startTime and endTime
        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("IST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("IST")));
        log.info("startLDT : {} , endLDT : {}, VALUE : {}", startLDT, endLDT, value);
    }

}
