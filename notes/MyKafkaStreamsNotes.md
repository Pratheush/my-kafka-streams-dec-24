
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







































































































































































































































