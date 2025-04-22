
### 4. Operators in Kafka Streams using KStream API

filter ::
![filter.png](screenshots%2F4.%20Operators%20in%20Kafka%20Streams%20using%20KStream%20API%2Ffilter.png)

filterNot ::
![filterNot.png](screenshots%2F4.%20Operators%20in%20Kafka%20Streams%20using%20KStream%20API%2FfilterNot.png)

map ::
![map.png](screenshots%2F4.%20Operators%20in%20Kafka%20Streams%20using%20KStream%20API%2Fmap.png)

mapValues ::
![mapValues.png](screenshots%2F4.%20Operators%20in%20Kafka%20Streams%20using%20KStream%20API%2FmapValues.png)

flatMap ::
![flatMap.png](screenshots%2F4.%20Operators%20in%20Kafka%20Streams%20using%20KStream%20API%2FflatMap.png)

flatMapValues ::
![flatMapValues.png](screenshots%2F4.%20Operators%20in%20Kafka%20Streams%20using%20KStream%20API%2FflatMapValues.png)

merge ::
![merge.png](screenshots%2F4.%20Operators%20in%20Kafka%20Streams%20using%20KStream%20API%2Fmerge.png)


***

### 5. Serialization and Deserialization in Kafka Streams

serdes ::
![serdes.png](screenshots%2F5.%20Serialization%20and%20Deserialization%20in%20Kafka%20Streams%2Fserdes.png)


what's needed to build a Custom Serde ?
* Serializer
* Deserializer
* Serde that holds the Serializer and Deserializer


### 7. Order Management Kafka Streams application - A real time use case
![Data Model For The Order.png](screenshots%2F7.%20Order%20Management%20Kafka%20Streams%20application%20-%20A%20real%20time%20use%20case%2FData%20Model%20For%20The%20Order.png)


### 8. Topology, Stream and Tasks - Under the Hood
![Topology.png](screenshots/8.%20Topology%2C%20Stream%20and%20Tasks%20-%20Under%20the%20Hood/Topology.png)

1. How Kafka Streams Executes Topology
![How KafkaStreams Executes Topology.png](screenshots/8.%20Topology%2C%20Stream%20and%20Tasks%20-%20Under%20the%20Hood/How%20KafkaStreams%20Executes%20Topology.png)

2. Tasks in Kafka Streams
![Tasks.png](screenshots/8.%20Topology%2C%20Stream%20and%20Tasks%20-%20Under%20the%20Hood/Tasks.png)
  * The benefit of creating tasks is that we can execute tasks in parallel by the Kafka Streams Application
  * Kakfa Streams splits data into partitions inside the topic and each and every partition is independent of one another so when you have four tasks created in this example you can parallely process them to speed up the overall process.
  * But the Parallelism in Kafka Streams Application is determined by Stream-Threads or we can also create multiple instances of Kafka-Stream Application.

3. Threads in Kafka Streams
![Threads in Kafka Streams.png](screenshots/8.%20Topology%2C%20Stream%20and%20Tasks%20-%20Under%20the%20Hood/Threads%20in%20Kafka%20Streams.png)

4. By Default There is no parallelism
![Default Stream Threads.png](screenshots/8.%20Topology%2C%20Stream%20and%20Tasks%20-%20Under%20the%20Hood/Default%20Stream%20Threads.png)
Let's say we have Kafka Topic with 4 partitions.
By Default There is no Parallelism in our Kafka Streams Application if Single instance of Kafka-Streams Application.

5. Parallelism Approach 1 in Kafka Stream Application
![Parallelism Approach 1.png](screenshots/8.%20Topology%2C%20Stream%20and%20Tasks%20-%20Under%20the%20Hood/Parallelism%20Approach%201.png)
   if our KAFKA-topic has 4 partitions and our kafka stream application is streaming the topic and we have 4 tasks		then by default only one streams-thread is going to execute each tasks.
   but if we increase the streams-thread by num.stream.threads property and seting up value to 4 i.e. 4 threads will be created with this setup all these 4 tasks are assigned to each and every threads and then each thread will execute the task individually and this is parallelism. this means that these 4 tasks execute the data from 4 partitions in parallel.

6. Parallelism Approach 1 in Kafka Stream Application
![Parallelism Approach 2.png](screenshots/8.%20Topology%2C%20Stream%20and%20Tasks%20-%20Under%20the%20Hood/Parallelism%20Approach%202.png)
But if num.stream.threads property and setting up value to 2 thus 2 stream threads are created and then 4 tasks will be evenly split between  the 2 stream threads and executed by two available stream threads. and this way we achieve parallelism

7. Parallelism Approach 2 By Multiple Instances of Kafka Stream Application
![Parallelism Apprach Multiple Instances.png](screenshots/8.%20Topology%2C%20Stream%20and%20Tasks%20-%20Under%20the%20Hood/Parallelism%20Apprach%20Multiple%20Instances.png)
If our KAFKA-topic has 4 partitions and our kafka stream application is streaming the topic and we have 4 tasks and if our kafka-streams applications multiple instances are running without setting up num.stream.threads property that means single stream-thread is running and executing tasks and this way we can achieve parallelism but each and every instance share the same application-id . in such setup tasks will be distributed to running multiple instances.

8. Ideal Number of Stream Threads
![Ideal Number of Stream Threads.png](screenshots/8.%20Topology%2C%20Stream%20and%20Tasks%20-%20Under%20the%20Hood/Ideal%20Number%20of%20Stream%20Threads.png)

![KafkaStreams Consumer Group.png](screenshots/8.%20Topology%2C%20Stream%20and%20Tasks%20-%20Under%20the%20Hood/KafkaStreams%20Consumer%20Group.png)

9. CHECK THE num.stream.threads is changed
we usually assign number of stream threads by setting Runtime.getRuntime().availableProcessors() at properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,Runtime.getRuntime().availableProcessors())
![Stream Threads Customize.png](screenshots/8.%20Topology%2C%20Stream%20and%20Tasks%20-%20Under%20the%20Hood/Stream%20Threads%20Customize.png)

### 9. ErrorException Handling in Kafka Streams
![Errors in Kafka Streams.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/Errors%20in%20Kafka%20Streams.png)

![ErrorHandlers In Kafka Streams.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/ErrorHandlers%20In%20Kafka%20Streams.png)

A typical Kafka Streams Application has three components ::
1. Deserialization
2. TOpology where our business logic resides
3. Serialization


if we have sink processor we uses serialization process to write the data into the output Kafka-Topic
this means that there are three places failures can happen

1. Deserialization or Transient Errors at the entry Level. (transient error means temporary issue which bcoz of network connection or partition rebalance)

2. RuntimeException in the Application Code (Topology) (any exception that our application run into due to logic)
3. Serialization or Transient Errors when producing the data.


###### ErrorHandlers in KAFKA Streams :::
For custom logic we can build our class implementing this ErrorHandler interfaces and wire that into our application

ERROR ::::														Error Handler
* Deserialization ::::												DeserializationExceptionHandler interface
* Application Error (At Topology Level)	::::			            StreamsUncaughtExceptionHandler interface
* Serialization	::::												ProductionExceptionHandler interface


### 10. KTable & Global KTable
![1. Introduction to KTable API.png](screenshots%2F10.%20KTable%20%26%20Global%20KTable%2F1.%20Introduction%20to%20KTable%20API.png)


![5. GlobalKTable.png](screenshots%2F10.%20KTable%20%26%20Global%20KTable%2F5.%20GlobalKTable.png)
In KTable
the tasks are split in between them because the data in the kafka topic in general split based on the keys since we have four partitions
we have keys split across all the four partitions so instance 1 has access to the only keys that are tied to the task 1 and task 2. it could be possibly data from the partition p1 and p2.
and instance 2 has access to the keys that are tied to task 3 and task 4.

In Global-KTable its instance have access to all the keys from all the tasks.
so it has way to get the data for all the keys from all the available instances and have the data available local to the instances.


### 11. StateFul Operations in Kafka Streams - Aggregate, Join and Windowing Events

   How Aggregation works ?
   Aggregations works only on Kafka Records that has non-null Keys.
   1. Group Records by Key
   2. Aggregate the Records

![How aggregation works .png](screenshots%2F11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events%2FHow%20aggregation%20works%20.png)


COUNT OPERATION ::

![Count Operator1.png](screenshots%2F11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events%2FCount%20Operator1.png)

REDUCE OPERATION ::

![Reduce Operator1.png](screenshots%2F11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events%2FReduce%20Operator1.png)

REDUCE OPERATION VISUALIZATION ::

![Reduce Operator Visualization.png](screenshots%2F11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events%2FReduce%20Operator%20Visualization.png)


AGGREGATE OPERATION ::
![Aggregate Operator.png](screenshots%2F11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events%2FAggregate%20Operator.png)

### 12. StateFul Operation Results - How to access them
approaches about sharing the data results of the aggregation
until now we have the aggregated results stored in the State-Store(RocksDB) and Internal Kafka Topic so these are the two places where aggregated data resides.
in order to be beneficial for the business data to be made available to the outside world or the teams inside the organization looking for that particular data.
1. OPTION 1: since state-store is RocksDB we can build the rest-api that interacts with the RocksDB and have the clients who look for the data to interact with this REST-Api
2. OPTION 2: Publishing the aggregated results in another Kafka-Topic and have the clients consume this data. if we are thinking that Internal Kafka-Topic has already data then why do we need to publish the results into another Kafka-Topic
Reason 1 is Kafka-Topic name is controlled by Kafka-Streams Library itself so we have limited control on what the Kafka Topic name the consumers needs to retrieve
from. In this option of publishing the data into another Kafka-Topic the client still needs to build the logic to read and update the aggregated results and this is my least favorite option so i am going to roll this one out.

Next favourable option is building the Rest-API and have the clients interact with the REST-Api but the Rest-API behind the scenes is going to interact with the State-Store i.e. RocksDB then fulfill the client request. This way client gets data directly from the source-app that's aggregating this data

![How to access the results of Aggregation .png](screenshots%2F12.%20StateFul%20Operation%20Results%20-%20How%20to%20access%20them%2FHow%20to%20access%20the%20results%20of%20Aggregation%20.png)


### 14. Re-Keying Kafka Records for Stateful operations
##### 2. Re-Keying using the selectKey operator

![Re-Keying using the selectKey operator.png](screenshots%2F14.%20Re-Keying%20Kafka%20Records%20for%20Stateful%20operations%2FRe-Keying%20using%20the%20selectKey%20operator.png)


### 15. StateFul Operations in Kafka Streams - Join
![Joining in KafkaStreams.png](screenshots%2F15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join%2FJoining%20in%20KafkaStreams.png)

* Different Types of Join that can be performed on Kafka-Streams.
Types of Joins in KAFKA Streams ::
Join Types 						Operators::
1. KStream-KTable				join, leftJoin (Joining KStream with the KTable)
2. KStream-GlobalKTable		    join, leftJoin  (Joining KStream with the GlobalKTable)
3. KTable-KTable				join,leftJoin,outerJoin  (Joining KTable with the KTable)
4. KStream-KStream 			    join, leftJoin, outerJoin  (Joining KStream with the KStream)

joins will get triggered if there is a matching record for the same key.
to achieve a resulting  data model like this, we would need a ValueJoiner.
this is also called innerJoin.
Join won't happen if the records from topics don't share the same key.

![Types of Join.png](screenshots%2F15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join%2FTypes%20of%20Join.png)

#### 2. Explore innerJoin using join operator - Joining KStream and KTable
1. we will create KStream out of Topic alphabet_abbreviations because this one can have different alphabets with different abbreviations
2. We will create KTable out of topic alphabets because Key A is always the first letter in the English Alphabet. and if its B then it is second letter.

so in case of KStream with KTable Join Operation
new events into the KTable doesn't trigger any join operation.
But new events into the KSTREAM always trigger join operation if there is matching key is found in KTable

![innerJoin using join operator - Joining KStream and KTable.png](screenshots%2F15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join%2FinnerJoin%20using%20join%20operator%20-%20Joining%20KStream%20and%20KTable.png)


#### 5. Explore innerJoin using join operator - Joining KStream and KStream

Join KStream - KStream ::::
 The KStream-KStream join is little different compared to the other ones.
 A KStream is an infinite stream which represents a log of everything that happened

 JoinWindows:::
 It is expected that they both share the same key, and also it should be in certain time window( there is a time window defined within the time window those events should be part of the KStream events )

 so by default any records that gets produced in the KAFKA Topic gets a timestamp attached to it


 what is the type of join-params
 StreamJoined<K,V1,V2> this class using which we can provide what the key-value and returned type is going to be

 if the primary stream begins a window within the 5-second window (here 5-second window is specified as JoinWindows ) which is back and forth which means like if the time is 5:00:00 of the event in primary stream then secondary stream event comes at 4:59:56 (4pm59 minutes and 56 seconds) and 5:00:04 (5 pm 00 minutes and 04 seconds )within that window back and forth
 if the event comes in the secondary stream then two events or messages from two KStreams will be joined when they both have same matching key

 Class used to configure the name of the join processor, the repartition topic name, state stores or state store names in Stream-Stream join.
 Type parameters: * <K> – the key type <V1> – this value type <V2> – other value type

 StreamJoined is a class that provides utility methods to define the serdes (serializer/deserializer) used when joining two KStreams or KTables.
 Serdes.String() is a built-in serde for handling strings. It’s being used here for both the keys and values of the streams or tables being joined.
 StreamJoined.with() is a static factory method that creates a new StreamJoined instance with the specified key, value, and other serdes.
 In this case, streamJoined is an instance of StreamJoined configured to use String serdes for both keys and values of the joining streams/tables.

![Join KStream-Kstream.png](screenshots%2F15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join%2FJoin%20KStream-Kstream.png)



#### 6. Joining Kafka Streams using leftJoin operator
![Left Join.png](screenshots%2F15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join%2FLeft%20Join.png)


#### 7. Joining Kafka Streams using outerJoin operator
![Outer Join.png](screenshots%2F15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join%2FOuter%20Join.png)



    KStream<String,String> alphabetKStream=streamsBuilder
      .stream(ALPHABETS,
         Consumed.with(Serdes.String(),Serdes.String()));

        alphabetKStream
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS-KSTREAM"));

        KStream<String,String>  alphabetAbbrevationsKStream=streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS,
                        Consumed.with(Serdes.String(),Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS-ABBREVIATIONS-KSTREAM"));

        ValueJoiner<String,String,Alphabet> alphabetValueJoiner=Alphabet::new;

        StreamJoined<String,String,String> paramJoins=StreamJoined.with(Serdes.String(),Serdes.String(),Serdes.String())
                //.withName(ALPHABET_TOPIC)
                //.withStoreName(ALPHABET_TOPIC)
                ;

        JoinWindows fiveSecondWindow=JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream.outerJoin(alphabetKStream,
                alphabetValueJoiner,
                fiveSecondWindow,
                paramJoins);

        joinedStream
                .print(Printed.<String,Alphabet>toSysOut().withLabel(JOINED_STREAM));


1. here when alphabetAbbrevationsKStream Topic is not receiving events then

       [JOINED-STREAM]: A, Alphabet[abbreviation=null, description=A is the FIRST letter in English Alphabets.]
       [JOINED-STREAM]: B, Alphabet[abbreviation=null, description=B is the SECOND letter in English Alphabets.]



2. here when alphabetKStream topic is not receiving events then

       [JOINED-STREAM]: A, Alphabet[abbreviation=Apple, description=null]
       [JOINED-STREAM]: B, Alphabet[abbreviation=Bus., description=null]
       [JOINED-STREAM]: C, Alphabet[abbreviation=Cat., description=null]


#### 9. CoPartitioning Requirements in Joins
![Co-Partioning Prerequisites in Join.png](screenshots%2F15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join%2FCo-Partioning%20Prerequisites%20in%20Join.png)

we can use selectKey or map operator to meet these requirements (in some scenarios number of partitions in source topics involved might differ then we can use selectKey() or map() means to 	re-key the records so that the records are going to be same part of the partitions)

![CoPartitioning In Join.png](screenshots%2F15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join%2FCoPartitioning%20In%20Join.png)

They are not been joined at all the reason why it is not happening its because they all are published into different topics so if we take a look into it  in alphabets_abbreviations
there are three partitions and three tasks created where as for alphabet there is only
one partition and one task created . since alphabets_abbreviations has three tasks and three
partitions data is getting published differently i.e. in different - different partitions as records
are evenly distributed across all the kafka topic partitions and  two source topics used for joins have different number of partitions so join was not triggered


OUTPUT CONSOLE ::: Join is not happening because two source topics have different number of partitions even though key for the related record is same.


[alphabets::12:19:42.658669]: A, A is the First letter in English Alphabets.
[alphabets::12:19:42.658669]: B, B is the Second letter in English Alphabets.
[alphabets_abbreviations::12:19:42.656668600]: B, Bus.
[alphabets_abbreviations::12:19:42.656668600]: C, Cat.
[alphabets_abbreviations::12:19:42.656668600]: A, Apple

### 16. Join in Order Management Application - A Real Time Use Case

#### 1. Join Aggregate Revenue with StoreDetails KTable
![Joins In Order Management Service.png](screenshots%2F16.%20Join%20in%20Order%20Management%20Application%20-%20A%20Real%20Time%20Use%20Case%2FJoins%20In%20Order%20Management%20Service.png)

### 17. StateFul Operations in Kafka Streams - Windowing
#### 1. Introduction to Windowing and time concepts
![windowing.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2Fwindowing.png)

![Time Concepts.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2FTime%20Concepts.png)

![TimeStamp Extractor in KafkaStreams.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2FTimeStamp%20Extractor%20in%20KafkaStreams.png)
1. FailOnInvalidTimestamp Extractor is the default. This is going to log.error() when an Invalid Timestamp occurs. here StreamException is thrown
2. LogAndSkipOnInvalidTimestamp extractor log.warn() when an invalid Timestamp occurs i.e. it will log the error and skip
3. Wallclock Timestamp Extractor is used when we don't have concern over the Timestamp, and we need the time when record gets processed by Streams Application.
4. 

![WallClockTimestamp Extractor.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2FWallClockTimestamp%20Extractor.png)

![window types.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2Fwindow%20types.png)

![Tumbling Window.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2FTumbling%20Window.png)

![Tumbling Window 2.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2FTumbling%20Window%202.png)

![RealTimeExampleTumblingWindow.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2FRealTimeExampleTumblingWindow.png)





















































































































































































