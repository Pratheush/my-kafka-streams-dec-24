package com.mylearning.orderkafkastreamsapp.serdes;


import com.mylearning.orderkafkastreamsapp.domain.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * this OrderSerdesFactory class is created to use it with Consumed.with() and Produced.with()
 * there in that in key section its String so Serdes.String() is used but in value section it is Greeting in JSON format
 * so we created the OrderSerdesFactory which has greeting() method which returns GreetingSerdes which is type of Serde<Greeting>
 *
 *     just like Serdes.String() method is used in Consumed.with() and Produced.with()
 *
 *     in Serdes.class
 *     public static Serde<String> String() {
 *         return new StringSerde();
 *     }
 *
 *     public static final class StringSerde extends WrapperSerde<String> {
 *         public StringSerde() {
 *             super(new StringSerializer(), new StringDeserializer());
 *         }
 *     }
 *
 *     public static class WrapperSerde<T> implements Serde<T> {} this WrapperSerde implements Serde of type that we specify as Target
 *
 */
public class OrderSerdesFactory {


    // in future if we want a different type we basically create another factory function then we change the type over here
    // like here we used Greeting we can use any other type accordingly
    public static Serde<Order> orderSerde(){
        JSONSerializer<Order> serializer = new JSONSerializer<>();
        JSONDeserializer<Order> deserializer=new JSONDeserializer<>(Order.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<Revenue> revenueSerde(){
        JSONSerializer<Revenue> serializer = new JSONSerializer<>();
        JSONDeserializer<Revenue> deserializer=new JSONDeserializer<>(Revenue.class);
        return Serdes.serdeFrom(serializer,deserializer);
    }

    public static Serde<TotalRevenue> totalRevenueSerde(){
        JSONSerializer<TotalRevenue> serializer = new JSONSerializer<>();
        JSONDeserializer<TotalRevenue> deserializer=new JSONDeserializer<>(TotalRevenue.class);
        return Serdes.serdeFrom(serializer,deserializer);
    }

    public static Serde<TotalRevenueWithAddress> totalRevenueWithAddressSerde(){
        JSONSerializer<TotalRevenueWithAddress> serializer =new JSONSerializer<>();
        JSONDeserializer<TotalRevenueWithAddress> deserializer=new JSONDeserializer<>(TotalRevenueWithAddress.class);
        return Serdes.serdeFrom(serializer,deserializer);
    }

    public static Serde<Store> storeSerde(){
        JSONSerializer<Store> serializer =new JSONSerializer<>();
        JSONDeserializer<Store> deserializer=new JSONDeserializer<>(Store.class);
        return Serdes.serdeFrom(serializer,deserializer);
    }
}
