
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

###### WHY KTABLE IS USED FOR STORE AND KSTREAM IS USED FOR ORDER ? ::
![Screenshot 2025-05-26 135358.png](screenshots/7.%20Order%20Management%20Kafka%20Streams%20application%20-%20A%20real%20time%20use%20case/Screenshot%202025-05-26%20135358.png)

![Screenshot 2025-05-26 135424.png](screenshots/7.%20Order%20Management%20Kafka%20Streams%20application%20-%20A%20real%20time%20use%20case/Screenshot%202025-05-26%20135424.png)

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


1. a. Default Deserialization Error Behavior without DESERIALIZATION ERROR HANDLER
![Deserialization Error Without Handler.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/Deserialization%20Error%20Without%20Handler.png)

1. b. Default Deserialization Error Behavior DEFAULT DESERIALIZATION ERROR HANDLER
![Default Deserialization Error Handlers.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/Default%20Deserialization%20Error%20Handlers.png)
* LogAndFailExceptionHandler
* LogAndContinueExceptionHandler

* LogAndContinueExceptionHandler.class implements DeserializationExceptionHandler if this one is default set then application will continue even at failures
* LogAndFailExceptionHandler implements DeserializationExceptionHandler  if this one is default set then application will fail and stop at failures

1. c. Default Deserialization Error Behavior OUTPUT CONSOLE VIEW SHOWING CONFIGURED LogAndFailExceptionHandler
![OutPutConsole View Showing LogAndFailExceptionHandler.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/OutPutConsole%20View%20Showing%20LogAndFailExceptionHandler.png)


1. d. Custom Deserialization Error Handler CONFIGURED CUSTOM DESERIALIZATION EXCEPTION-HANDLER
![Custom DeserializationExceptionHandler.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/Custom%20DeserializationExceptionHandler.png)

![Configured Custom DeserializationExceptionHandler.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/Configured%20Custom%20DeserializationExceptionHandler.png)

1. e. Custom Deserialization Error Handler CONFIGURED CUSTOM DESERIALIZATION EXCEPTION-HANDLER
![CUSTOM DeserializationExceptionHandler Class.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/CUSTOM%20DeserializationExceptionHandler%20Class.png)


1. f. 4. Default & Custom Processor Error Handler CUSTOM StreamsUncaughtExceptionHandler
![Custom StreamsUncaughtExceptionHandler.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/Custom%20StreamsUncaughtExceptionHandler.png)
* REPLACE_THREAD(0, "REPLACE_THREAD") :::::::::::
  This is going to do provide a new thread for the error and this is going to constantly read write that failed message until that becomes recovered.
* SHUTDOWN_CLIENT(1, "SHUTDOWN_KAFKA_STREAMS_CLIENT")
  this is going to shut down the particular thread thats causing the problem
* SHUTDOWN_APPLICATION(2, "SHUTDOWN_KAFKA_STREAMS_APPLICATION");
  this is going to shut down the whole application lets say we have two tasks both the tasks will be shutdown.


1. g. 4. Default & Custom Processor Error Handler CONFIGURED CUSTOM StreamsUncaughtExceptionHandler
![Configured Custom StreamsUncaughtExceptionHandler.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/Configured%20Custom%20StreamsUncaughtExceptionHandler.png)
   * StreamsUncaughtExceptionHandler(HERE we created custom StreamProcessorCustomErrorHandler) are added differently than DeserializationExceptionHandler.
   * we need to add this StreamProcessorCustomErrorHandler into topology itself. so we place the instance of the StreamProcessorCustomErrorHandler into the KafkaStreams instance itself kafkaStream.setUncaughtExceptionHandler();


1. h. 4. Default & Custom Processor Error Handler SIMULATION OF EXCEPTION AT TOPOLOGY WHERE IS DATA IS PROCESSED INSIDE exploreErrors() METHOD StreamsUncaughtExceptionHandler
![StreamsUncaughtExceptionHandler Simulation.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/StreamsUncaughtExceptionHandler%20Simulation.png)

* the otherway of handling this is to have error handling directly in the topology itself (in my case i applied try-catch around IllegalStateException() where "Transient Error" is thrown)

* this way you application is still up and running in this way we dont have to deal with this  StreamProcessorCustomErrorHandler itself
do have this logic if you want to shutdown the application because if we dont bring the application down it may lead to some kind of corrupt data in your application and in those kind of scenarios
shuting down the applicaiton is the best option. but the alternative is if you wana ignore any error the better approach is have the error handler in the topology itself


1. i. 5. Custom Production Error Handler CUSTOM PRODUCTION EXCEPTION HANDLER
![Custom ProductionExceptionHandler.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/Custom%20ProductionExceptionHandler.png)


1. j. 5. Custom Production Error Handler CONFIGURED PRODUCTION EXCEPTION HANDLER
![Configured Production ExceptionHandler.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/Configured%20Production%20ExceptionHandler.png)

* if Kafka Cluster goes down and our KAFKA Stream Application is up and running then it retries to connect again and again our KAFKA Stream Application consumer and producer shows dissconnected but still try to reconnect

* in AdminClientConfig  there is retries value i.e. highest integer value 2147483647
* in ProducerConfig value there is retries value i.e. highest integer value 2147483647

### 10. KTable & Global KTable
![1. Introduction to KTable API.png](screenshots%2F10.%20KTable%20%26%20Global%20KTable%2F1.%20Introduction%20to%20KTable%20API.png)
KTable holds the latest value for a given key and old value for a given key will be lost.
* How to create a KTable ?
```
public static Topology build(){

		StreamsBuilder streamsBuilder= new StreamsBuilder();
		
		KTable<String,String> wordsTable=streamsBuilder
				     .table("words", Consumed.with(Serdes.String(), Serdes.String()), 																						
				     Materialized.as("words-store")); // storeName
																																		
}
```

![How To create KTable.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/How%20To%20create%20KTable.png)

* Materialized.as("words-store") creates stateStore for us.
* StateStore is necessary for storing the data in an additional entity.
* Materialized.as("words-store") creates a stateStore.
  This is needed to retaining the data in an app crash or restart or redeployment.
* in the event of app-crash or application-restart or redeployment then using the stateStore we need to retrieve the previous state of the KTable.
* The default state store that comes with KakfaStreams is RocksDB
* RocksDB is a high performance embedded database for storing key-value data. This is an open source DB under the Apache 2.0 license
* Data in the embedded key-value store is also persisted in a file system.
* Data in the RocksDB also maintained in a changelog topic for Fault Tolerance. ChangeLog Topic fundamentally helps in rebuilding the data of the KTable when app crashes or restarts or redeployment. This way state of the application is read from the changelog topic which is internal and local to KafkaStreams and then it gets rebuild.
* KafkaStreams uses another entity behind the scenes to manage the data for our application and we can query the data in stateStore.
* When to use KTable ?
  Any business usecase that requires the streaming app to maintain the latest value for a given key can benefit from KTable.

Example :
Stock Trading App that requires to maintain the latest value for a given Stock symbol.

* During	app-crash or application-restart or redeployment the state of the application is read from the internal topic i.e. ChangeLog Topic which is local to Kafka-Stream and then it gets rebuild.
* KEY-POINT :::
  1. without Materialized.as("words-store") i.e. state-store ktable will get all the key-value data to the downstream nodes but with state-store only the latest data i.e. key-value is available. to the downstream node.
  2. i.e. Materialized.as("words-store") ktable buffers the record for certain timeframe and once the timeframe is exhausted
  3. ktable publish the message or data to the downstream so we will get only the latest value here

```
// WITH Materialized.as("words-store") shows only the latest key-value pair data in downstream operation.

10:36:00.864 [ktable-b57a88a9-be0c-4f85-9589-81c126da1b51-StreamThread-1] INFO  c.m.g.topology.ExploreKTableTopology - wordsTable :: key :: A, value :: Ambulance
[ktable-words]: A, Ambulance
10:36:00.865 [ktable-b57a88a9-be0c-4f85-9589-81c126da1b51-StreamThread-1] INFO  c.m.g.topology.ExploreKTableTopology - wordsTable :: key :: B, value :: Baby
[ktable-words]: B, Baby




CONSOLE OUTPUT ::::: 
// WITHOUT Materialized.as("words-store") : shows all the key-value pair data
10:38:37.684 [ktable-5c9f30de-c060-4ace-99eb-99e272578435-StreamThread-1] INFO  c.m.g.topology.ExploreKTableTopology - wordsTable :: key :: A, value :: Apple
[ktable-words]: A, Apple
10:38:37.691 [ktable-5c9f30de-c060-4ace-99eb-99e272578435-StreamThread-1] INFO  c.m.g.topology.ExploreKTableTopology - wordsTable :: key :: A, value :: Alligator
[ktable-words]: A, Alligator
10:38:37.691 [ktable-5c9f30de-c060-4ace-99eb-99e272578435-StreamThread-1] INFO  c.m.g.topology.ExploreKTableTopology - wordsTable :: key :: A, value :: Ambulance
[ktable-words]: A, Ambulance
10:38:37.694 [ktable-5c9f30de-c060-4ace-99eb-99e272578435-StreamThread-1] INFO  c.m.g.topology.ExploreKTableTopology - wordsTable :: key :: B, value :: Bus
[ktable-words]: B, Bus
10:38:37.695 [ktable-5c9f30de-c060-4ace-99eb-99e272578435-StreamThread-1] INFO  c.m.g.topology.ExploreKTableTopology - wordsTable :: key :: B, value :: Baby
[ktable-words]: B, Baby



CONSOLE OUTPUT ::::
when key is null then ::: THEN RECORD IS IGNORED.
10:42:51.165 [ktable-5c9f30de-c060-4ace-99eb-99e272578435-StreamThread-1] WARN  o.a.k.s.k.internals.KTableSource - Skipping record due to null key. topic=[ktable-words] partition=[0] offset=[15]
10:42:51.182 [ktable-5c9f30de-c060-4ace-99eb-99e272578435-StreamThread-1] WARN  o.a.k.s.k.internals.KTableSource - Skipping record due to null key. topic=[ktable-words] partition=[0] offset=[16]
10:42:51.189 [ktable-5c9f30de-c060-4ace-99eb-99e272578435-StreamThread-1] WARN  o.a.k.s.k.internals.KTableSource - Skipping record due to null key. topic=[ktable-words] partition=[0] offset=[17]
10
10:42:51.198 [ktable-5c9f30de-c060-4ace-99eb-99e272578435-StreamThread-1] INFO  c.m.g.topology.ExploreKTableTopology - wordsTable :: key :: B, value :: Bus
[ktable-words]: B, Bus
10:42:51.204 [ktable-5c9f30de-c060-4ace-99eb-99e272578435-StreamThread-1] INFO  c.m.g.topology.ExploreKTableTopology - wordsTable :: key :: B, value :: Baby
[ktable-words]: B, Baby

```
* WITHOUT Materialized.as("words-store") : shows all the key-value pair data
![Without Materialized.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/Without%20Materialized.png)
* WITH Materialized.as("words-store") shows only the latest key-value pair data in downstream operation.
![With Materialized.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/With%20Materialized.png)
* when key is null then ::: THEN RECORD IS IGNORED.
![When Key Is Null.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/When%20Key%20Is%20Null.png)

* once the message is consumed by this KTable what it does is it keeps buffering for certain time frame and then once again time frame is exhausted then what it will do is
it is going to take the latest value for the given key and then publish that message downstream

* so in this case the KTable waits for certain time frame and buffers the record with in the time frame and once the time frame is exhausted its going to sending that message downstream so
in this cases its our filter() operator , toStream() and then print() gets executed based on the data.
* so KTable is an API we use when we want to see latest value for any given key.

3. KTable - Under the Hood
![KTable COnfiguration for Deciding When To emit data.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/KTable%20COnfiguration%20for%20Deciding%20When%20To%20emit%20data.png)

* cache.max.bytes.buffering
![cach max bytes buffering.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/cach%20max%20bytes%20buffering.png)
* Caching also helps with the managing the amount of data written into the RocksDB
* default size of Buffer is 10MB. i.e. cache.max.bytes.buffering=10485760(~10MB)
* so this cache holds the value in the memory and once the memory is full then it will decide to send the value to the downstream nodes.
  this is one way of managing the KTable to prevent sending the values to the downstream nodes immediately


* commit.interval.ms
![commit interval ms.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/commit%20interval%20ms.png)
  Since we have used Materialized.as() any value in the KTable is going to be maintained in the internal changelog topic.


* internal changelog topic
![internal changelog topic.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/internal%20changelog%20topic.png)


* values in the internal changelog topic
![values in the internal changelog topic.png](screenshots/9.%20ErrorException%20Handling%20in%20Kafka%20Streams/values%20in%20the%20internal%20changelog%20topic.png)

when we restart the application what this application is going to do is this is going to Kafka-topic and get
all the values for the state store which is mentioned in Materialized.as() i.e. words-store, and it's going to match the name  of state-store and adds the changelog topic at the end thus internal changelog topic name would be like ktable-words-store-changelog. so when this KTable gets created it knows what is the previous value for the key A and B  .so its going to read all the values for the given key and value and its going to get the updated value for the given key.

whenever we use KTable with Materialized.as() this is going to create internal changelog topic and this is the topic which is mainly used for fault-tolerance in the case of application restart or crash this is how its going to get the latest value for the given key and any time we get an update for this given keys it knows what was the previous value and it will go ahead and update the value.

KTables are backed by RocksDB for local storage and a changelog topic in Kafka for resilience

* How to create GlobalKTable
![GlobalKTable Code.png](screenshots/10.%20KTable%20%26%20Global%20KTable/GlobalKTable%20Code.png)




* KTable vs GlobalKTable
![5. GlobalKTable.png](screenshots%2F10.%20KTable%20%26%20Global%20KTable%2F5.%20GlobalKTable.png)
Let's say here we have two instances of Application sharing the application.id each application instances has one stream-thread to execute tasks and since source topic has 4 partitions so 4 tasks will be created behind the scenes and two tasks will get evenly distributed to each stream-thread of an application instance and stream-thread will execute the tasks thus parallelism is achieved.


In KTable :: 
If we use KTable then the tasks are split in between the instances because the data in the kafka topic in general split based on the keys since we have four partitions
we have keys split across all the four partitions so instance 1 has access to the only keys that are tied to the task 1 and task 2. it could be possibly data from the partition p1 and p2.
and instance 2 has access to the keys that are tied to task 3 and task 4.

In Global-KTable its instance have access to all the keys from all the tasks.
so it has way to get the data for all the keys from all the available instances and have the data available locally to the instances.



* When To USe KTable or GlobalKTable
![When To Use KTable GlobalKTable.png](screenshots/10.%20KTable%20%26%20Global%20KTable/When%20To%20Use%20KTable%20GlobalKTable.png)



### 11. StateFul Operations in Kafka Streams - Aggregate, Join and Windowing Events
![Stateful Operations in Kafka Streams.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/Stateful%20Operations%20in%20Kafka%20Streams.png)

* Stateful Operators in Kafka Streams.
![Stateful Operators in Kafka Streams.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/Stateful%20Operators%20in%20Kafka%20Streams.png)


* Aggregation of Data
![Aggregations of Data.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/Aggregations%20of%20Data.png)
  
*  How Aggregation works ?
   Aggregations works only on Kafka Records that has non-null Keys.
   1. Group Records by Key
   2. Aggregate the Records

![How aggregation works .png](screenshots%2F11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events%2FHow%20aggregation%20works%20.png)


COUNT OPERATION ::

![Count Operator1.png](screenshots%2F11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events%2FCount%20Operator1.png)

* Count Operation Visualization
![count operation.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/count%20operation.png)


![Count Operator 2.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/Count%20Operator%202.png)

![Count Operator3.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/Count%20Operator3.png)

![Output for Count Operator.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/Output%20for%20Count%20Operator.png)

* The internal topic gets created and The aggregated value gets stored in an internal changelog topic.
![Internal Changelog Topic Count Operator.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/Internal%20Changelog%20Topic%20Count%20Operator.png)
aggregate-KSTREAM-AGGREGATE-STATE-STORE-000000002-changelog. In the Internal topic this is where all the data resides.
* aggregate is the application name
* KSTREAM-AGGREGATE-STATE-STORE that gets created

when we restart the application then how does the application reconstruct the previous value for the given key.
so The application reconstructs the whole state of the Topology meaning for the given key the value reconstructed by reading through kafka topic which is this internal changelog topic
i.e. aggregate-KSTREAM-AGGREGATE-STATE-STORE-000000002-changelog.
so every time value for the given key changes value gets updated in the internal topic.

* groupBy() IS USED TO RE-KEY THE RECORD OR WE CAN USE IT IF THE KAFKA RECORD HAS NO KEY THEN WE CAN USE groupBy() TO CREATE A NEW KEY.
![GroupBy Usage with Count Operator.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/GroupBy%20Usage%20with%20Count%20Operator.png)
* new internal changelog topic is created as we change the key using groupBy()

![changelog and repartition topic.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/changelog%20and%20repartition%20topic.png)
* repartition is the concept where in distributed environment repartition is going to take care of placing the record in the appropriate partitions so any time we change the key in this case we are using groupBy() operation then we have two internal topics are created one is changelog and second is repartition. 


REDUCE OPERATION ::

![Reduce Operator1.png](screenshots%2F11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events%2FReduce%20Operator1.png)
inside reduce() operator it accepts previous value and current value.

REDUCE OPERATION VISUALIZATION ::

![Reduce Operator Visualization.png](screenshots%2F11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events%2FReduce%20Operator%20Visualization.png)

* USAGE OF reduce() OPERATOR IN CODE ::
![explore reduce operator.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/explore%20reduce%20operator.png)


* reduce() operator output console view
 ![reduce operator output console.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/reduce%20operator%20output%20console.png)


AGGREGATE OPERATION ::
![Aggregate Operator.png](screenshots%2F11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events%2FAggregate%20Operator.png)

AGGREGATE USAGE ::
![Aggregate Usage.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/Aggregate%20Usage.png)
* The Initializer is basically a Java Bean which is going to represent Json that had three properties and we are basically initating the new instance of it.The type of its going to be Initializer (AlphabetWordAggregate).
* Aggregator Code : Aggregator is where we are going to be updating the running count and update the array value with the new values as the new values come in.
* Materializing the aggregated value to a state store as this is neeeded anytime the application is restarted and the application needs to reconstruct the old state. The reason why we use materialize specifically in this usecase and not for the other one is that the type here is going to be different because the value is going to be a new object or a new type its not goin to be String anymore i.e. one of the reason why we are using materialize over here.

EXPLORE AGGREGATE :: exploreAggregate()
![EXPLORE AGGREGATE 1.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/EXPLORE%20AGGREGATE%201.png)

![EXPLORE AGGREGATE 2.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/EXPLORE%20AGGREGATE%202.png)

![AlphabetWordAggregateSerde.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/AlphabetWordAggregateSerde.png)

AlphabetWordAggregate
![AlphabetWordAggregate 1.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/AlphabetWordAggregate%201.png)

AlphabetWordAggregate EXPLANATION
![AlphabetWordAggregate 2.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/AlphabetWordAggregate%202.png)

AGGREGATE OPERATOR OUTPUT CONSOLE VIEW :: 
![Aggregate Output Console View.png](screenshots/11.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Aggregate%2C%20Join%20and%20Windowing%20Events/Aggregate%20Output%20Console%20View.png)


### 12. StateFul Operation Results - How to access them
* The aggregated results store in state-store i.e. rocksDB which is high performance embedded key-value store and the internal kafka topic i.e. changelog topic. These are the two places where results of aggregation resides. 
* RocksDB is local to the KafkaStream App
* Changelog topic is a kafka topic used to store the state of the local RocksDB instances when stateful processing happens KafkaStream backs up the local RocksDB store to a kafka topic known as Changelog topic which ensures fault tolerance and allows recovery in case of failures.
* approaches about sharing the data results of the aggregation
until now we have the aggregated results stored in the State-Store(RocksDB) and Internal Kafka Topic so these are the two places where aggregated data resides.
in order to be beneficial for the business data to be made available to the outside world or the teams inside the organization looking for that particular data.
1. OPTION 1: since state-store is RocksDB we can build the rest-api that interacts with the RocksDB and have the clients who look for the data to interact with this REST-Api
2. OPTION 2: Publishing the aggregated results in another Kafka-Topic and have the clients consume this data. if we are thinking that Internal Kafka-Topic has already data then why do we need to publish the results into another Kafka-Topic
Reason 1 is Kafka-Topic name is controlled by Kafka-Streams Library itself so we have limited control on what the Kafka Topic name the consumers needs to retrieve
from. In this option of publishing the data into another Kafka-Topic the client still needs to build the logic to read and update the aggregated results and this is my least favorite option so i am going to roll this one out.

Next favourable option is building the Rest-API and have the clients interact with the REST-Api but the Rest-API behind the scenes is going to interact with the State-Store i.e. RocksDB then fulfill the client request. This way client gets data directly from the source-app that's aggregating this data

![How to access the results of Aggregation .png](screenshots%2F12.%20StateFul%20Operation%20Results%20-%20How%20to%20access%20them%2FHow%20to%20access%20the%20results%20of%20Aggregation%20.png)

### 13. Aggregation in Order Management Application - A Real Time Use Case
TWO BUSINESS REQUIREMENTS :: 
![Two Business Requirements.png](screenshots/13.%20Aggregation%20in%20Order%20Management%20Application%20-%20A%20Real%20Time%20Use%20Case/Two%20Business%20Requirements.png)

BUSINESS LOGIC FOR ABOVE TWO BUSINESS REQUIREMENTS :: 

```
/** ValueMapper: Used to map a value to a new value without modifying the key. It is a stateless operation applied to each record individually.
 *
 * ValueMapperWithKey: Similar to ValueMapper, but it also has access to the key while transforming the value.
 * This allows for transformations that depend on both the key and value.
 *
 * KeyValueMapper: Used when both the key and value need to be transformed. This is useful when you want to modify the key along with the value
 *
 * The ValueJoiner interface for joining two values into a new value of arbitrary type. This is a stateless operation
 */

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
                    .table(STORES, Consumed.with(Serdes.String(),OrderSerdesFactory.storeSerde()),
                        Materialized.<String,Store,KeyValueStore<Bytes,byte[]>>as(STORES));

        storeKTable
                .toStream()
                        .print(Printed.<String,Store>toSysOut().withLabel(STORES));

        //splitUsingBranched(orderStream, generalPredicate, restaurantPredicate);

        return streamsBuilder.build();
    }

// Predicate<? super String, ? super Order> generalPredicate, Predicate<? super String, ? super Order> restaurantPredicate
    //   tutorial way of doing the split of OrderStream into Two General and Restaurant and produce to two different kafka-topics
    private static void splitUsingBranched(KStream<String, Order> orderStream, Predicate<? super String, ? super Order> generalPredicate, Predicate<? super String, ? super Order> restaurantPredicate) {

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
    }

```


### 14. Re-Keying Kafka Records for Stateful operations
##### 1. Effect of null Key in Stateful Operations & Repartition of Kafka Records
![effects of null key in stateful operation.png](screenshots/14.%20Re-Keying%20Kafka%20Records%20for%20Stateful%20operations/effects%20of%20null%20key%20in%20stateful%20operation.png)

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
![KTable for Alphabets KStreams for Alphabet Abbreviations.png](screenshots/15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join/KTable%20for%20Alphabets%20KStreams%20for%20Alphabet%20Abbreviations.png)
* KTable is used when maintaining the latest state while KStream is used for processing each event separately.
  The details that don't change frequently and if it is changed or updated there is only latest value is important then use  KTable

* KTable used for things that have an updated state (latest value for the given key where previous value is updated or overwritten for the given key) without storing historical changes.

* KStream used for things that are event-driven and need history means every record is a new event and processed separately so there is no need to overwrite the previous one.

1. we will create KStream out of Topic alphabet_abbreviations because this one can have different alphabets with different abbreviations
2. We will create KTable out of topic alphabets because Key A is always the first letter in the English Alphabet. and if its B then it is second letter.

so in case of KStream with KTable Join Operation
new events into the KTable doesn't trigger any join operation.
But new events into the KSTREAM always trigger join operation if there is matching key is found in KTable

![innerJoin using join operator - Joining KStream and KTable.png](screenshots%2F15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join%2FinnerJoin%20using%20join%20operator%20-%20Joining%20KStream%20and%20KTable.png)

```
/**
     * joins will get triggered if there is a matching record for the same key.
     * to achieve a resulting  data model like this, we would need a ValueJoiner.
     * this is also called innerJoin.
     * Join won't happen if the records from topics don't share the same key.
     *
     * so in case of join operation KStream with KTable
     * new events into the KTable doesn't trigger any join
     * but new events into the KSTREAM always trigger join if there is matching key is found in KTable
     * @param streamsBuilder
     */
    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder){

        KStream<String,String> alphabetAbbrevationsKStream=streamsBuilder
                        .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS_ABBREVIATIONS-KSTREAM"));


        KTable<String,String> alphabetKTable=streamsBuilder
                .table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String())
                        , Materialized.as("ALPHABETS-STORE"));

        alphabetKTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("ALPHABETS-KTABLE"));


        //<V1> – first value type <V2> – second value type <VR> – joined value type
        ValueJoiner<String, String, Alphabet> alphabetValueJoiner = Alphabet::new;

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream
                                    .join(alphabetKTable,alphabetValueJoiner);

        // [alphabets-with-abbreviations]: A, Alphabet[abbreviation=Apple, description=A is the first letter in English Alphabets.]
        // [alphabets-with-abbreviations]: B, Alphabet[abbreviation=Bus, description=B is the second letter in English Alphabets.]
        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel("ALPHABETS-WITH-ABBREVIATIONS"));
    }

```

#### 3. Explore innerJoin using join operator - Joining KStream and GlobalKTable

* Joining a KStream with a GlobalKTable in Apache Kafka Streams is a powerful way to enrich streaming data with reference data.

![Diff KStream GlobalKTable.png](screenshots/15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join/Diff%20KStream%20GlobalKTable.png)

![KeyValueMapper ValueJoiner.png](screenshots/15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join/KeyValueMapper%20ValueJoiner.png)

![Example KStream GlobalKTable.png](screenshots/15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join/Example%20KStream%20GlobalKTable.png)

* Step 1: Define Data Models :: 

![DataModel for Join KStream GlobalKTable.png](screenshots/15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join/DataModel%20for%20Join%20KStream%20GlobalKTable.png)

* Step 2: Create a Kafka Streams Topology ::
```
StreamsBuilder builder = new StreamsBuilder();

// Define KStream for orders
KStream<String, Order> ordersStream = builder.stream("orders-topic", Consumed.with(Serdes.String(), new JsonSerde<>(Order.class)));

// Define GlobalKTable for customer details
GlobalKTable<String, Customer> customersTable = builder.globalTable("customers-topic", Consumed.with(Serdes.String(), new JsonSerde<>(Customer.class)));

// Perform Join Operation
KStream<String, EnrichedOrder> enrichedStream = ordersStream.join(
    customersTable,
    (orderId, order) -> order.getCustomerId(),  // KeyValueMapper: Extract customerId from Order
    (order, customer) -> new EnrichedOrder(order.getOrderId(), customer.getCustomerId(), customer.getName(), customer.getAddress(), order.getAmount()) // ValueJoiner: Merge Order and Customer details
);

// Output enriched orders
enrichedStream.to("enriched-orders-topic", Produced.with(Serdes.String(), new JsonSerde<>(EnrichedOrder.class)));

KafkaStreams streams = new KafkaStreams(builder.build(), config);
streams.start();

```

![Breakdown for Example Kstream GlobalKtable.png](screenshots/15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join/Breakdown%20for%20Example%20Kstream%20GlobalKtable.png)

KeyValueMapper ::
![KeyValueMapper.png](screenshots/15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join/KeyValueMapper.png)

ValueJoiner ::
![ValueJoiner.png](screenshots/15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join/ValueJoiner.png)

MY CODE EXAMPLE :::: 
```
/**
     * Here it works same as KStream and KTable Joining
     * However when we are joining KStream with GlobalKTable then We need KeyValueMapper and ValueJoiner the reason is that GlobalKTable is the representation of all the data that's part of the KAFKA Topic
     * it's not about a specific instance holding set of keys based on partition that particular task interacts with it's going to have whole representation in those kind of scenarios we need to provide
     * KeyValueMapper that's going to represent what the key is going to be in this case
     * @param streamsBuilder
     */

    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder){

        var alphabetAbbrevationsKStream=streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS_ABBREVIATIONS-KSTREAM"));


        var alphabetGlobalKTable=streamsBuilder
                .globalTable(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabets-store"));

        // GlobalKTable has no toStream() method
        /*alphabetGlobalKTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));*/


        /**
         * when we are joining KStream with GlobalKTable then We need KeyValueMapper and ValueJoiner the reason is that GlobalKTable is the representation of all the data that's part of the KAFKA Topic
         * it's not about a specific instance holding set of keys based on partition that particular task interacts with it's going to have whole representation in those kind of scenarios we need to provide
         * KeyValueMapper that's going to represent what the key is going to be in this case
         */
        // <K> – key type <V> – value type <VR> – mapped value type ; :::: here below this leftKey is key from KStream i.e. alphabetAbbrevationsKStream
         KeyValueMapper<String,String,String> keyValueMapper= (leftKey, alphabetAbbrevationValue) -> leftKey;

        //<V1> – first value type from KStream <V2> – second value type from GlobalKTable <VR> – joined value type
        ValueJoiner<String, String, Alphabet> alphabetValueJoiner= Alphabet::new;
        // ValueJoiner<String, String, Alphabet> alphabetValueJoiner= (stringAlphabetAbrevationValue, stringAlphabetDescriptionValue) -> new Alphabet(stringAlphabetAbrevationValue, stringAlphabetDescriptionValue);

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream
                .join(alphabetGlobalKTable,keyValueMapper,alphabetValueJoiner);

        // [JOINED-STREAM]: A, Alphabet[abbreviation=Apple, description=A is the first letter in English Alphabets.]
        // [JOINED-STREAM]: B, Alphabet[abbreviation=Bus, description=B is the second letter in English Alphabets.]
        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel(JOINED_STREAM));
    }

```

#### 4. Explore innerJoin using join operator - Joining KTable and KTable
```
 /**
     * In this usecase either side of the data or new event is going to trigger join operation, in our case :::: alphabets_abbreviations table and alphabet table both are KTables so irrespective of whether the data is going to
     * be sent to this alphabets_abbreviations table or alphabet table there will be join triggered.
     * if new event is published in first KTable and no new events Published in second KTable then join operation will be triggered and only that number of events producer results with the matching key
     * if new event is published in both KTable then join operation will produce result with number of total events from both KTable with matching key will produce results.
     * @param streamsBuilder
     */
    private static void joinKTableWithKTable(StreamsBuilder streamsBuilder){

        KTable<String,String> alphabetAbbrevationsKTable=streamsBuilder
                .table(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.<String, String, KeyValueStore<Bytes,byte[]>>as("ALPHABETS-ABBREVIATIONS-STORE"));

        alphabetAbbrevationsKTable
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS-ABBREVIATIONS-KTABLE"));


        KTable<String,String> alphabetKTable=streamsBuilder
                .table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("ALPHABETS-STORE"));

        alphabetKTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("ALPHABETS-KTABLE"));


        //<V1> – first value type <V2> – second value type <VR> – joined value type
        ValueJoiner<String, String, Alphabet> alphabetValueJoiner= Alphabet::new;

        KTable<String,Alphabet> joinedStream=alphabetAbbrevationsKTable
                .join(alphabetKTable,alphabetValueJoiner);

        /**
         * irrespective of whether the data or event is present in ALPHABETS_ABBREVIATIONS Topic (KTable1) or
         * data is present in ALPHABETS Topic (KTable2) it's going to trigger join operation
         * thus getting output produced for that number of events occurred i.e. if two events arrived or present in topic1 then it will trigger join operation two times and
         * when two more events arrived or present in topic2 then it will trigger join operation again two more times thus produced output will be four times.
         */
        // [JOINED-STREAM]: A, Alphabet[abbreviation=Apple, description=A is the first letter in English Alphabets.]
        // [JOINED-STREAM]: B, Alphabet[abbreviation=Bus, description=B is the second letter in English Alphabets.]
        // [JOINED-STREAM]: B, Alphabet[abbreviation=Bus, description=B is the second letter in English Alphabets.]
        // [JOINED-STREAM]: A, Alphabet[abbreviation=Apple, description=A is the first letter in English Alphabets.]
        joinedStream
                .toStream()
                .print(Printed.<String, Alphabet>toSysOut().withLabel(JOINED_STREAM));
    }
    
```


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

![Joining Two KStreams.png](screenshots/15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join/Joining%20Two%20KStreams.png)

![Understanding JoinWindows.png](screenshots/15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join/Understanding%20JoinWindows.png)
```
JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10));

```
![Understanding StreamJoined.png](screenshots/15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join/Understanding%20StreamJoined.png)
```
StreamJoined<String, Order, Payment> streamJoined = StreamJoined.with(Serdes.String(), new JsonSerde<>(Order.class), new JsonSerde<>(Payment.class));

```

###### 3. Example Code for Joining Two KStreams
Let’s say we want to join an Orders KStream (orders-topic) with a Payments KStream (payments-topic) based on the same orderId
```
StreamsBuilder builder = new StreamsBuilder();

// Define KStream for Orders
KStream<String, Order> ordersStream = builder.stream("orders-topic", Consumed.with(Serdes.String(), new JsonSerde<>(Order.class)));

// Define KStream for Payments
KStream<String, Payment> paymentsStream = builder.stream("payments-topic", Consumed.with(Serdes.String(), new JsonSerde<>(Payment.class)));

// Perform Windowed Join
KStream<String, EnrichedOrder> enrichedOrdersStream = ordersStream.join(
    paymentsStream,
    (order, payment) -> new EnrichedOrder(order.getOrderId(), order.getCustomerId(), order.getAmount(), payment.getPaymentMethod(), payment.getStatus()),
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),  // Events must occur within 10 seconds
    StreamJoined.with(Serdes.String(), new JsonSerde<>(Order.class), new JsonSerde<>(Payment.class)) // Serialization formats
);

// Output enriched orders
enrichedOrdersStream.to("enriched-orders-topic", Produced.with(Serdes.String(), new JsonSerde<>(EnrichedOrder.class)));

KafkaStreams streams = new KafkaStreams(builder.build(), config);
streams.start();



```

![Keypoints Joining KStreams.png](screenshots/15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join/Keypoints%20Joining%20KStreams.png)

MY CODE :::: 

```
private static void joinKStreamWithKStream(StreamsBuilder streamsBuilder){
        // Primary
        var alphabetAbbrevationsKStream=streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetAbbrevationsKStream
                .print(Printed.<String,String>toSysOut().withLabel("ALPHABETS-ABBREVIATIONS-KSTREAM" +"::"+streamTimeStamp()));

        // Secondary
        var alphabetKStream=streamsBuilder
                .stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetKStream
                .print(Printed.<String, String>toSysOut().withLabel("ALPHABETS-KSTREAM" +"::"+streamTimeStamp()));

        /**
         * Join KStream - KStream ::::
         * The KStream-KStream join is little different compared to the other ones.
         * A KStream is an infinite stream which represents a log of everything that happened
         *
         * JoinWindows:::
         * It is expected that they both share the same key, and also it should be in certain time window( there is a time window 
         * defined within the time window those events should be part of the KStream events )
         *
         * so by default any records that gets produced in the KAFKA Topic gets a timestamp attached to it
         *
         * what is the type of join-params
         * StreamJoined<K,V1,V2> this class using which we can provide what the key-value type and value type of other stream is going to be
         *
         * if the primary stream begins and a window defined is the 5-second window (here 5-second window is specified as JoinWindows ) which is back 
         * and forth which means like if the primary or first event occurred at o time is 5:00:00 of the event in primary stream then secondary stream 
         * event should be occurring comes in between 4:59:56 (4pm59 minutes and 56 seconds) and 5:00:05 (5 pm 00 minutes and 05 seconds )within that window back and forth
         * if the second event comes in the secondary stream then two events or messages from two KStreams will be joined when they both have same matching key
         */

        //<V1> – first value type <V2> – second value type <VR> – joined value type
        ValueJoiner<String, String, Alphabet> alphabetValueJoiner= Alphabet::new;

        JoinWindows fiveSecondWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        /**
         * 
         * StreamJoined is a class that provides utility methods to define the serdes (serializer/deserializer) used when joining two KStreams.
         * Serdes.String() is a built-in serde for handling strings. It’s being used here for both the keys and values of the streams that are been joined.
         * StreamJoined.with() is a static factory method that creates a new StreamJoined instance with the specified key, value, and other serdes.
         * In this case, streamJoined is an instance of StreamJoined configured to use String serdes for both keys and values of the joining streams/tables.
         *
         *  StreamJoined this is the class using which we can provide what the key-value and the return type is going to be
         *  first argument is the Key Type we are dealing with Alphabet-Abbreviations-Kstream
         *  second argument is the Value Type we are dealing with Alphabet-Abbreviations-Kstream
         *  the third argument is the type of the value we are dealing with Alphabet-Stream
         */
        StreamJoined<String, String, String> joinedParams = StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String());

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream
                .join(alphabetKStream,alphabetValueJoiner,fiveSecondWindow,joinedParams);

        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel(JOINED_STREAM+"::"+streamTimeStamp()));

    }

```

#### 6. Joining Kafka Streams using leftJoin operator
![Left Join.png](screenshots%2F15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join%2FLeft%20Join.png)
MY CODE :::
```

private static void joinKStreamWithKStreamWithLeftJOIN(StreamsBuilder streamsBuilder){
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
                .withName("ALPHABETS-JOINS")
                .withStoreName("ALPHABETS-JOINS");

        JoinWindows fiveSecondWindow=JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream
                                                    .leftJoin(alphabetKStream,
                                                        alphabetValueJoiner,
                                                        fiveSecondWindow,
                                                        paramJoins);

        /**
         * [JOINED-STREAM]: C, Alphabet[abbreviation=Cat., description=null]
         * if there is no matching record on the right side, then the join will be triggered with null value for the right side value
         * here right is Alphabet with key = A, value = A is the first letter in English Alphabets
         * here left is Alphabet-Abbreviations with key = A, value = Apple
         * when both sides matching records then it will trigger join work as usual.
         */
        joinedStream
                .print(Printed.<String,Alphabet>toSysOut().withLabel(JOINED_STREAM));

    }

```


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


#### 8. Join - Under the hood
```

   /**StreamJoined Configuration:
     * StreamJoined is a builder class that configures parameters for joining kafka streams
     * It's configured with key, value, and store serdes (serializer/deserializer) using Serdes.String() for both keys and values.
     * withName() and withStoreName() methods are used to name the joined stream and specify the store name respectively
     * withName(ALPHABET_TOPIC) is used to set the name of the joined stream. This name will be used internally within Kafka Streams.
     * 
     * Kafka Streams allows you to store intermediate results of stream processing in state stores. so specify the store-name to take control of creating internal topic where 
     * intermediate results are stored. if we don't specify withStoreName() with StreamJoined.with() then kafka will create internal topic and name the topic by itself.
     * creating internal topic this way is recommended approach.
     */

  KStream<String,String> alphabetKStream=streamsBuilder
                .stream(ALPHABETS,
                        Consumed.with(Serdes.String(),Serdes.String()));

        KStream<String,String>  alphabetAbbrevationsKStream=streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS,
                        Consumed.with(Serdes.String(),Serdes.String()));

        ValueJoiner<String,String,Alphabet> alphabetValueJoiner=Alphabet::new;

        StreamJoined<String,String,String> paramJoins=StreamJoined.with(Serdes.String(),Serdes.String(),Serdes.String())
                .withName(ALPHABET_TOPIC)
                .withStoreName(ALPHABET_TOPIC);

        JoinWindows fiveSecondWindow=JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        KStream<String,Alphabet> joinedStream=alphabetAbbrevationsKStream.outerJoin(alphabetKStream,
                alphabetValueJoiner,
                fiveSecondWindow,
                paramJoins);
```

#### 9. CoPartitioning Requirements in Joins
![Co-Partioning Prerequisites in Join.png](screenshots%2F15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join%2FCo-Partioning%20Prerequisites%20in%20Join.png)

same partition strategy should be used when we are publishing the data to the topic.

we can use selectKey or map operator to meet these requirements (in some scenarios number of partitions in source topics involved might differ then we can use selectKey() or map() means to 	re-key the records so that the records are going to be same part of the partitions)

![CoPartitioning In Join.png](screenshots%2F15.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Join%2FCoPartitioning%20In%20Join.png)
Which of these types of Joins must have co-partitioning requirements ?
Except the KStream and GlobalKTable type of join all the joins require the co-partitioning prerequisite. Therefore, The related records that we are trying to join from multiple different topics should have the same number of Kafka partitions in the topic then should also be keyed on the same element.

EXAMPLE ::
They are not been joined at all the reason why it is not happening its because they all are published into different topics so if we take a look into it  in alphabets_abbreviations
there are three partitions and three tasks created where as for alphabet there is only
one partition and one task created . since alphabets_abbreviations has three tasks and three
partitions data is getting published differently i.e. in different - different partitions as records
are evenly distributed across all the kafka topic partitions and  two source topics used for joins have different number of partitions so join was not triggered


OUTPUT CONSOLE ::: Join is not happening because two source topics have different number of partitions even though key for the related record is same.

```
[alphabets::12:19:42.658669]: A, A is the First letter in English Alphabets.
[alphabets::12:19:42.658669]: B, B is the Second letter in English Alphabets.
[alphabets_abbreviations::12:19:42.656668600]: B, Bus.
[alphabets_abbreviations::12:19:42.656668600]: C, Cat.
[alphabets_abbreviations::12:19:42.656668600]: A, Apple
```

### 16. Join in Order Management Application - A Real Time Use Case

#### 1. Join Aggregate Revenue with StoreDetails KTable
![Joins In Order Management Service.png](screenshots%2F16.%20Join%20in%20Order%20Management%20Application%20-%20A%20Real%20Time%20Use%20Case%2FJoins%20In%20Order%20Management%20Service.png)




### 17. StateFul Operations in Kafka Streams - Windowing
#### 1. Introduction to Windowing and time concepts
![windowing.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2Fwindowing.png)

![Time Concepts.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2FTime%20Concepts.png)
Processing Time : This is the time when records get read and processed by streams application. Also called Consumer Processing Time.

![TimeStamp Extractor in KafkaStreams.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2FTimeStamp%20Extractor%20in%20KafkaStreams.png)
1. FailOnInvalidTimestamp Extractor is the default. This is going to log.error() when an Invalid Timestamp occurs. here StreamException is thrown
2. LogAndSkipOnInvalidTimestamp extractor log.warn() when an invalid Timestamp occurs i.e. it will log the error and skip. The key point is that it doesn't throw exception it simply logs the exception in case of Invalid Timestamp.
3. Wallclock Timestamp Extractor is used when we don't have concern over the Timestamp, and we need the time when record gets processed by Streams Application.

![WallClockTimestamp Extractor.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2FWallClockTimestamp%20Extractor.png)
Wallclock Timestamp Extractor completely ignores the consumer record and its going to give us timestamp as System.currentTimeMillis().

![window types.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2Fwindow%20types.png)
* Windowing means grouping the records together based on certain defined time window 

#### 2. Windowing in Kafka Streams - Tumbling Windows
![Tumbling Window.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2FTumbling%20Window.png)

![Tumbling Window 2.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2FTumbling%20Window%202.png)

![RealTimeExampleTumblingWindow.png](screenshots%2F17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing%2FRealTimeExampleTumblingWindow.png)
when we produce data even though we are not passing the timestamp,so current machines Timestamp gets added before data is published into the topic.

Anytime windows are created going to represent GMT time not the local time. Always pass the GMT time in order to query against this data.

```
private static void tumblingWindow(KStream<String, String> wordKStream) {

        Duration fiveSecondWindowSize=Duration.ofSeconds(5);

        TimeWindows timeWindows=TimeWindows.ofSizeWithNoGrace(fiveSecondWindowSize);

        // KTable is of type KTable<Windowed<String>,Long> so the Key <Windowed<String> of this windowedBy() operation.
        KTable<Windowed<String>, Long> windowedKTable = wordKStream
                .groupByKey()
                .windowedBy(timeWindows)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        // in reality, we can publish the windowedKTable to downstream Kafka-Topic. here we are just printing in console.
        // to show the timestamp of each and every window gets created. we can see what the windowed key is look like which is of type windowed<String>
        // and long value is the count of each and every key in the given window.
        windowedKTable
                .toStream()
                .peek((key, value) -> {
                    log.info("tumblingWindow :: key : {} , value : {}",key,value);
                    // going to print the local date time
                    printLocalDateTimes(key,value);
                })
                .print(Printed.<Windowed<String>,Long>toSysOut().withLabel(WINDOW_WORDS));
    }
    
    // each window has startTime and endTime
    private static void printLocalDateTimes(Windowed<String> key, Long value) {
        Instant startTime = key.window().startTime(); // startTime type is Instant
        Instant endTime = key.window().endTime();   // any time windows are created it is going to be in gmt time not the localTime
        log.info("startTime : {}, endTime : {}, Count : {}", startTime, endTime, value); // here printing the instant startTime and endTime  :: windowed key startTime and endTime are in gmt format

        // converting the instant value into local timestamp using zone since i am in IST i.e entry("IST", "Asia/Kolkata"),. we can get the code by clicking into SHORT_IDS
        // getting the localtime from the startTime and endTime
        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("IST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("IST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }

```
#### 3. Control emission of windowed results using supress operartor

![SuppressionConfig.png](screenshots/17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing/SuppressionConfig.png)
the behavior we are looking for is anytime the defined window we have for the window is completed we want those results emitted downstream.use operator suppress() which can be used to suppress or buffer the records until the time interval for the time window
is complete. in order to achieve this we need Suppression COnfig, Buffer Config and BufferFull Config.
 
![BufferConfig BufferFullConfig.png](screenshots/17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing/BufferConfig%20BufferFullConfig.png)
*  if memory is full then this BufferConfig.unbounded() will throw an out-of-memory exception.
*  emitEarlyWhenFull this is going to make sure the app is up and processing the records but if the buffer is full its going to emit the results.

![suppression implementation.png](screenshots/17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing/suppression%20implementation.png)
Above implementation has three configs
1. Suppressed.untilWindowCloses()
2. Suppressed.BufferConfig.unbounded() is recommended option since there is no way we know how much buffer size is going to be 
3. shutDownWhenFull() when buffer is full shutdown the app.

Suppressed.BufferConfig.unbounded() means this can store infinite number of bytes  i.e. expectation
and shutdown the app when the buffer is full.

```
private static void tumblingWindow(KStream<String, String> wordKStream) {

        Duration fiveSecondWindowSize=Duration.ofSeconds(5);

        TimeWindows timeWindows=TimeWindows.ofSizeWithNoGrace(fiveSecondWindowSize);

        // KTable is of type KTable<Windowed<String>,Long> so the Key <Windowed<String> of this windowedBy() operation.
        KTable<Windowed<String>, Long> windowedKTable = wordKStream
                .groupByKey()
                .windowedBy(timeWindows)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        // in reality, we can publish the windowedKTable to downstream Kafka-Topic. here we are just printing in console.
        // to show the timestamp of each and every window gets created. we can see what the windowed key is look like which is of type windowed<String>
        // and long value is the count of each and every key in the given window.
        /**
         * since usage of suppress() makes sure that records are going to be sent downstream only when the window is exhausted.
         */
        windowedKTable
                .toStream()
                .peek((key, value) -> {
                    log.info("tumblingWindow :: key : {} , value : {}",key,value);
                    // going to print the local date time
                    printLocalDateTimes(key,value);
                })
                .print(Printed.<Windowed<String>,Long>toSysOut().withLabel(WINDOW_WORDS));
    }
    
```
* so now with the help of suppress() operator we are able to emit the results of windowed aggregation as per our window size this is benefit of using suppress(). The suppress() operator takes control of when the results are emitted downstream so this way we are not tied to the commit.interval.ms setting . with this advantage here is that we get the aggregated data the format we would expect to the downstream operation.
* we can publish this data and this value also get stored in a state-store we can expose the data to the rest-api
* using suppress() is beneficial when our application is required to publish the aggregated data based on window-size.

#### 4. Windowing in Kafka Streams - Hopping Windows
![Hopping Window.png](screenshots/17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing/Hopping%20Window.png)

![Hopping Window Visualization.png](screenshots/17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing/Hopping%20Window%20Visualization.png)
Here we have 5 seconds window buckets. first window is created with the 5 seconds window and then all the aggregations will be performed based on the keys that are present in that 5 second window. and next one is going to be overlapping window where start of the window is going to be 3 seconds from the starting window of the first window start time. and the reason for 3 seonds is because advance size is 3 seconds and this process continues for the remaining window buckets. this is overlapping window so the records are going to be overlapped between window buckets
```
private static void hoppingWindow(KStream<String, String> wordKStream) {

        Duration fiveSecondWindowSize = Duration.ofSeconds(5);
        Duration advanceBySize = Duration.ofSeconds(3);

        TimeWindows timeWindows=TimeWindows
                .ofSizeWithNoGrace(fiveSecondWindowSize)
                .advanceBy(advanceBySize);

        // KTable is of type KTable<Windowed<String>,Long> so the Key <Windowed<String> of this windowedBy() operation.
        KTable<Windowed<String>, Long> windowedKTable = wordKStream
                .groupByKey()
                .windowedBy(timeWindows)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        // in reality, we can publish the windowedKTable to downstream Kafka-Topic. here we are just printing in console.
        // to show the timestamp of each and every window gets created. we can see what the windowed key is look like which is of type windowed<String> and long value is the count of each and every key in the given window.
        windowedKTable
                .toStream()
                .peek(((key, value) -> {
                    log.info("hoppingWindow :: key : {} , value : {}",key,value);
                    // going to print the local date time
                    printLocalDateTimes(key,value);
                }))
                .print(Printed.<Windowed<String>,Long>toSysOut().withLabel(WINDOW_WORDS));
    }
    
    // each window has startTime and endTime
    private static void printLocalDateTimes(Windowed<String> key, Long value) {
        Instant startTime = key.window().startTime(); // startTime type is Instant
        Instant endTime = key.window().endTime();   // any time windows are created it is going to be in gmt time not the localTime
        log.info("startTime : {}, endTime : {}, Count : {}", startTime, endTime, value); // here printing the instant startTime and endTime  :: windowed key startTime and endTime are in gmt format

        // converting the instant value into local timestamp using zone since i am in IST i.e entry("IST", "Asia/Kolkata"),. we can get the code by clicking into SHORT_IDS
        // getting the localtime from the startTime and endTime
        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("IST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("IST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }

```
CONSOLE OUTPUT VIEW :: Observe startLDT for each aggregated result to downstream print operation.
![HoppingWindow Console Output.png](screenshots/17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing/HoppingWindow%20Console%20Output.png)

#### 5. Windowing in Kafka Streams - Sliding Windows
![Sliding Window.png](screenshots/17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing/Sliding%20Window.png)
the timestamp attached to kafka record thats been published is the one going to create window.

![Sliding Window Visualization.png](screenshots/17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing/Sliding%20Window%20Visualization.png)
EXPLANATION ::
lets say we have sliding window of duaration of 5 seconds. lets say we recieve event at the 6 seond so event as Key "A" in this case sliding window will be created with the event minue time define in the sliding window. in this case its 5 seconds so a new window will be created from the first seocnd to the sixth seocnd. and the count of A is 1. here we are performing windowed aggregation based on the count lets say we recieve another event at 7th second now a new window will be created with the start time as 2( because 7-5 is 2 and at 7 event occured and 5 second is the window bucket) and end time as 7 and count of key "A" will be 2 in this case. now we recieve event at the 13the second which is far from the previous two windows in this case a new window will be created with the start time as 8 second as (13-5 is 8 ) and end-time as 13the second and the count of "A" will be 1 in this case because in this 5 seconds window only one key "A" is occured so count is 1.

* so its the event that drives the window bucket. 
* windows that are created can overlap if there are events within the defined window 
* so use ths sliding window when we have the use case to create windows in small increments of time in those kindo of scenarios we use sliding window
  another use-case is if we are not going to be reliant on the machines clock time and we want the events to drive actual time windows then in those kind of scenarios we can use sliding window

```
private static void slidingWindow(KStream<String, String> wordKStream) {

        Duration fiveSecondWindowSize = Duration.ofSeconds(5);

        SlidingWindows slidingWindow = SlidingWindows.ofTimeDifferenceWithNoGrace(fiveSecondWindowSize);

        // KTable is of type KTable<Windowed<String>,Long> so the Key <Windowed<String> of this windowedBy() operation.
        KTable<Windowed<String>, Long> windowedKTable = wordKStream
                .groupByKey()
                .windowedBy(slidingWindow)
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        // in reality, we can publish the windowedKTable to downstream Kafka-Topic. here we are just printing in console.
        // to show the timestamp of each and every window gets created. we can see what the windowed key is look like which is of type windowed<String> and long value is the count of each and every key in the given window.
        windowedKTable
                .toStream()
                .peek(((key, value) -> {
                    log.info("slidingWindow :: key : {} , value : {}",key,value);
                    // going to print the local date time
                    printLocalDateTimes(key,value);
                }))
                .print(Printed.<Windowed<String>,Long>toSysOut().withLabel(WINDOW_WORDS));
    }


private static void printLocalDateTimes(Windowed<String> key, Long value) {
        Instant startTime = key.window().startTime(); // startTime type is Instant
        Instant endTime = key.window().endTime();   // any time windows are created it is going to be in gmt time not the localTime
        log.info("startTime : {}, endTime : {}, Count : {}", startTime, endTime, value); // here printing the instant startTime and endTime  :: windowed key startTime and endTime are in gmt format

        // converting the instant value into local timestamp using zone since i am in IST i.e entry("IST", "Asia/Kolkata"),. we can get the code by clicking into SHORT_IDS
        // getting the localtime from the startTime and endTime
        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("IST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("IST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }

```
* Producer is producing event at every second so :::
OBSERVE first message the event occurred time . Here sliding time window bucket is 5 seconds.
![first message produced.png](screenshots/17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing/first%20message%20produced.png)
OBSERVE second message the event consumed time here its endLDT timing.
![second message consumed or processed.png](screenshots/17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing/second%20message%20consumed%20or%20processed.png)
in second message :::  endLDT 16:26:32 - 5 i.e. 16:26:27 which is startLDT and count is 1

![third message.png](screenshots/17.%20StateFul%20Operations%20in%20Kafka%20Streams%20-%20Windowing/third%20message.png)
in third message :::  endLDT 16:26:33 - 5 i.e. 16:26:28 which is startLDT and count is 2

### 18. Widowing in Order Management Application - A Real Time Use Case

![requirement.png](screenshots/18.%20Widowing%20in%20Order%20Management%20Application%20-%20A%20Real%20Time%20Use%20Case/requirement.png)

![json message.png](screenshots/18.%20Widowing%20in%20Order%20Management%20Application%20-%20A%20Real%20Time%20Use%20Case/json%20message.png)
* the timestamp is embedded in itself in this case we need to build our custom timestamp extractor to extract the timestamp from the message and supply it to our application. so that the time-windows will be created appropriately based on the time in this message instead of the kafka-record publish time.

CUSTOM TIMESTAMP EXTRACTOR ::
```
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


```
TOPOLOGY METHOD WITH AGGREGATION AND JOIN
```
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

```

















































































































































