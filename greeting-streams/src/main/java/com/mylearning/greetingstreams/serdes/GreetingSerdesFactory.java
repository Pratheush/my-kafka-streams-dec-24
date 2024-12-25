package com.mylearning.greetingstreams.serdes;


import com.mylearning.greetingstreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * This class has bunch of static functions
 * This will take care of providing the serializer and deserializer that is expected by the serde
 * this GreetingSerdesFactory class is created to use it with Consumed.with() and Produced.with()
 * there in that in key section its String so Serdes.String() is used but in value section it is Greeting in JSON format
 * so we created the GreetingSerdesFactory which has greeting() method which returns GreetingSerdes which is type of Serde<Greeting>
 *
 *    Serde<String> == Serde<Greeting>
 *    WrapperSerde<T> == GreetingSerdes
 *    StringSerde
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
public class GreetingSerdesFactory {

    private GreetingSerdesFactory() {}
    public static Serde<Greeting> greetingSerde(){

        return new GreetingSerde();
    }

    // in future if we want a different type we basically create another factory function then we change the type over here
    // like here we used Greeting we can use any other type accordingly
    public static Serde<Greeting> greetingUsingGenerics(){
        JSONSerializer<Greeting> serializer = new JSONSerializer<>();
        JSONDeserializer<Greeting> deserializer=new JSONDeserializer<>(Greeting.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
