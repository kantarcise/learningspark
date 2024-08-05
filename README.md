# learningspark

All the exercises from the book "[Learning Spark](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)", solved with Dataframes and Datasets, no stone unturned.

## Why ?

There are a lot of sources on Spark, like [LearningApacheSpark](https://github.com/runawayhorse001/LearningApacheSpark) or [learning-spark](https://github.com/databricks/learning-spark) from Databricks and it's [second version](https://github.com/databricks/LearningSparkV2/tree/master). Sadly, they are not recently updated. 

You have to wander a lot, just to get the ball rolling, especially if you are new to the environment.

I thought, it shouldn't be this hard.

## Scala ? 🤔 I don't know any Scala 😕

Focus for 90 minutes and finish the [Prelude꞉ A Taste of Scala](https://docs.scala-lang.org/overviews/scala-book/prelude-taste-of-scala.html). After that, if you still want to discover more, you can check out [Tour of Scala](https://docs.scala-lang.org/tour/tour-of-scala.html). You can test your understanding [in this website - Scastie](https://scastie.scala-lang.org/).

## Usage 🔨

You have 3 choices:

### Instruction Route - Instructions 🎫 

- You can follow the instructions to make yourself a Spark playground.

### Cherry Pick Route - Code Catalog 📚

- You can only look for what you are interested in. Dataframe API, Dataset API, SparkStreaming etc.

### Template Route - Use as Template 💭

- You can simply use all of the code in your projects as a template. This may help you by acting like a [catalyst.](https://www.britannica.com/science/catalyst)

Choose as you wish. 

> [!IMPORTANT]
> Almost all of the code was configured so that it could be run in IDE. If you are working with a cluster manager, you should adjust the code accordingly. If this does not make a lot of sense for you, don't worry and keep reading on!

---

Here are all the details for routes:

### Instructions 🎫 
 
This repository is tested on

```
- Ubuntu 22.04
- Java 11
- Scala 2.12.18
- Spark 3.5.0
- sbt 1.9.6
```

1) You can follow [this video](https://www.youtube.com/watch?v=-AXBg3sk6II) to install Apache Spark.

Down below are all the steps taken in the video. If you get stuck, you can refer back to the video.

Prerequisites:

- Install Java 11 and [add it to path](https://tecadmin.net/adding-directory-to-path-variable-in-linux/). Wait, what was [JDK, JRE, JVM again ?](https://www.geeksforgeeks.org/differences-jdk-jre-jvm/)

- Install Scala 2.18.12 from the source and add it to path. Why are we using [Scala again](https://www.projectpro.io/article/why-learn-scala-programming-for-apache-spark/198)?

- Install Sbt 1.9.6 and add it to path. What is [sbt ?](https://www.scala-sbt.org/1.x/docs/) 

- Install Spark 3.5.0, unzip it. Add it to path that you unzipped. Should you download the one with or without Hadoop ? Here is [the answer](https://stackoverflow.com/questions/48514247/if-i-already-have-hadoop-installed-should-i-download-apache-spark-with-hadoop-o).

Now you should be able to use:

```bash
spark-shell
```

and you are ready to roll!

2) For development we will use [Intellij IDEA](https://www.jetbrains.com/idea/). It has a 30 day trial period, so no worries.

Download Intellij IDEA and follow the `readme` inside compressed file:

Which is simply summarized as: 

- To start the application, open a console, cd into "{your installation home}/bin" and type:

```bash
./idea.sh
```
To open the IDE. You can make a [Desktop Shortcut](https://askubuntu.com/a/1145563) and add it to your Favorites.

3) To setup a project from scratch, select `New Project`, select JDK, Scala and SBT versions that we installed, and [choose a package prefix](https://blog.jetbrains.com/scala/2020/11/26/enhanced-package-prefixes/) you prefer.

4) Now you are ready to try out some code!

### Code Catalog 📚

Here is all the code in this repository, explained in detail. After you cloned the repo, you can simply make a project from existing source with it!

#### Chapter 0 - A Fast Start (Not a Part of the Book)

- If you want to just see what are Dataset's and Dataframe's in Spark, you can check out [Aggregates](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/Aggregates.scala). What does aggregates mean, you ask? Well, continue to find out. 😌

- Also if you have some familiartiy to Python, you can check out the difference between Type Annotations in Scala and Type Hints in Python, [in this note](https://github.com/kantarcise/learningspark/blob/main/reading/TypeAnnotationsVsTypeHints.md).

#### Chapter 1 - Introduction to Apache Spark: A Unified Analytics Engine

- This chapter is just a reading assignment. I think it is important to learn the history/roots of tools and also have a high level picture what they might look like, as we try to master them.

- If you want to hear the story from GPT-4o, you can check out the [note here](https://github.com/kantarcise/learningspark/blob/main/reading/GFStoSparkStory.md).

#### Chapter 2 - Downloading Apache Spark and Getting Started

- In our first example, we'll load some data onto Spark and try to answer some questions based on the data! Processed sugar is terrible and you know it, however, here is an [example on MnM's](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/MnmCandies.scala) In this application, we'll discover about relative paths (`System.getProperty("user.dir")`), user inputs (`args(0)`) and Dataframe Schemas.

- Also here is the same example, using [only Datasets](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/MnmCandiesDataset.scala)! This is our first contact with `as[]` method to make Datasets and some typed transformations like `groupByKey` and a class like `Aggregator`. We will use them repeatedly, so do not worry if you do not understand them right away! Also, what we mean when we are making a Application with Dataset's is that we are exclusively using the [Typed Transformations](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html).

#### Chapter 3 - Apache Spark’s Structured APIs

- In Chapter 3 we will work with a JSON file about bloggers in Databricks. Let's see who is the most articulate, with [Bloggers](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/Bloggers.scala)! This application will show us most common ways we can work on a [Column](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html) in a Dataframe! `toDF()` will be discovered too! 

- We will also have a typed transformations only version, [here](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/BloggersDataset.scala). Here we can discover about making a Dataset from a collection like `Seq` with case classes. We'll also see [how we can get a schema from a case class](https://github.com/kantarcise/learningspark/blob/main/reading/DatasetsWithSchemaFromCaseClass.md), different ways of interacting with cols (`expr`, `$`, `col("someColumnName")`) a simple example of sorting.

- After that, we will discover more about the [sf-fire-calls.csv](https://github.com/kantarcise/learningspark/blob/main/data/sf-fire-calls.csv) data. This will be an opportunity to discover a common typecast between String to Timestamp. Here is our simple appetizer, [FireCalls](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FireCalls.scala).

- In [FireCallsDataset](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FireCallsDataset.scala) we will only use typed transformations, and make use of `map` and `orderby`. We cannot use `unix_timestamp` and `to_timestamp` ([functions doc](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html)), because they return [Column's](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html).

- For the tests of this application, In [FireCallsTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/FireCallsTest.scala) we will see how to mock data with `Some`, explained [in this notebook here](https://github.com/kantarcise/learningspark/blob/main/reading/OptionTypeAndSome.md). 

- For our FireCalls CSV data, we have some questions that are solved in the book, so we will go over them to understand in [FireCallsSolvedQuestions](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FireCallsSolvedQuestions.scala). In this app, we will see how to save our data as Parquet files & Tables, and some function usage like `distinct()`, `countDistinct()`, `withColumn`, `withColumnRenamed`, `lit()` and `datesub()`. These are among the most resource-rich in terms of API usage and discovery.

- Also [FireCallsSolvedQuestionsDataset](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FireCallsSolvedQuestionsDataset.scala) includes some unique methods like `.filter(m => !m.CallType.contains("Medical Incident"))` , `.filter(ct => ct.CallType.isDefined)`, `map()` followed by `reduce()` and `groupByKey()` followed by `mapGroups()`. This application may be one of the most complex Dataset one we have! 🥳

- Finally, for the FireCalls CSV data, we will solve all the training questions from the book. In [FireCallsTrainingQuestions](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FireCallsTrainingQuestions.scala) we have an end to end pipeline, using `to_timestamp`, `lit()` some columns ourselves, `timestamp - where().where().where()` or `timestamp - between()`, `weekofyear()`,  `saveAsTable()` and parallel write to filesystem. Also, we have seen enough examples to understand `where() == filter()` and `distinct()  == dropDuplicates()`.

- [FireCallsTrainingQuestionsDataset](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FireCallsTrainingQuestionsDataset.scala) may be the most complex Dataset example we have! We used a lof od different `Aggregators`, used `map()`, `filter()`, `groupByKey()` extensively. 🍉

- Then we will get to the [IoT Devices data](https://github.com/kantarcise/learningspark/blob/main/data/iot_devices.json), as a JSON. We will start with a basic workout on Datasets, discovering the differencee between Lambdas and DSL expressions (Page 170 in the book), with [IoTDatasetFunctionComparison](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/IoTDatasetFunctionComparison.scala).

- If you want to read more about how should we decide between [Typed and Untyped transformations](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html) in our Datasets, you can read [this note](https://github.com/kantarcise/learningspark/blob/main/reading/TypedVSUntypedTransformations.md).

- At the end of Chapter 3, we will practice our skills on the IoT Devices data and discover more with [IoTDataframeWorkout](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/IoTDataframeWorkout.scala) as an End to End pipeline. This will teach us about `collect`, `take`, `first`, `.stat.approxQuantile` and different types of combinations for `groupBy - agg - orderBy` and `groupBy - orderBy` on multiple cols. We will test all of our approaches, [here.](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/IotDatasetWorkoutTest.scala)

- [IotDatasetWorkout](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/IotDatasetWorkout.scala) will show us all the untyped transformations we did, in typed transformations version! This one is a great practice for a unique `Aggregator`, `map() - reduce()` and a replicate of `stat.approxQuantile` - `getQuantile`! 🎩

#### Chapter 4 - Spark SQL and DataFrames: Introduction to Built-in Data Sources

- In Chapter 4 we will discover about Spark SQL, which is literally [SQL](https://aws.amazon.com/what-is/sql/#:~:text=Structured%20query%20language%20(SQL)%20is,relationships%20between%20the%20data%20values.) stuff. 

- We'll use a data about [Flight Delays](https://github.com/kantarcise/learningspark/blob/main/data/departuredelays.csv), a CSV file. [FlightDelays](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FlightDelays.scala) will help us discover discover more about, views & tables, union Dataframes, using `when().when().when().when().otherwise()` instead of using multiple unions, `explain()` to see the plans, and a sneak peak to `freqItems` -> `df.stat.freqItems(Seq("origin"))` 

- Also, the [Dataset version](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FlightDelaysDataset.scala) simply requires a case class like [Flights](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/Flights.scala) so that we can use typed transformations.

- On mocked data, you can see our techniques tested in [FlightDelaysTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/FlightDelaysTest.scala).

#### Chapter 5 - Spark SQL and DataFrames: Interacting with External Data Sources

- In this chapter, we again work on Flight Delay data, but we will discover different things! With [FlightDelaysAdvanced](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FlightDelaysAdvanced.scala) we will learn about making Tables - `createOrReplaceTempView`, getting rid of `expr()` and understanding it's use case (`expr()` function to use SQL syntax anywhere a column would be specified.). 

- We will see about casting types, like `.withColumn("delay", $"delay".cast(IntegerType))`, filtering multiple things like `filter($"a" === 1 && $"b" === 2)`, `.startsWith("")`, joins (inner join), `.isin("a", "b", "c")`, window functions. We will see functions like `dense_rank`, `drop - rename - pivot`. A different type cas is also waiting us, strings to time -> `02190925 - 02-19 09:25 am` , `.withColumn("month", $"date".substr(0, 2).cast(IntegerType))`.

- The `main()` methods are getting really long at this point. If you want some help decomposing the code, you can see an example at [FlightDelaysAdvancedDecomposed](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FlightDelaysAdvancedDecomposed.scala). Tests for such decomposed code are at [FlightDelaysAdvancedTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/FlightDelaysAdvancedTest.scala).

- Finally, we will see a simple example to understand windowing, with [SimpleWindowing](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/SimpleWindowing.scala).

#### Chapter 6 - Spark SQL and Datasets

- In Chapter 6, we will try to understand the dynamics of Memory Management for Datasets and DataFrames. For that, we have some mocked data and we will try to understand the DSL vs Lambda usage, with [DSLVersusLambda](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/DSLVersusLambda.scala)

- Before closing, we will have a small Dataset Workout on Internet Usage data which is made on fly (in the code), and we will practice what we have learned so far in [InternetUsage](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/InternetUsage.scala)

- For the tests of InternetUsage, we can use a concept called Traits to enrich our skillset, which [can be seen here](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/InternetUsageTest.scala). 

#### Chapter 7 - Optimizing and Tuning Spark Applications

- After all the code we have written, is there some tricks we can learn to make our apps more efficient? Turns out, there are a lot of them! In [ConfigurationsAndCaching](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/ConfigurationsAndCaching.scala) we will discover about printing Configurations, dynamicAllocation, setting configurations on the fly, seeing spark.sql configs,  caching/persisting, and partioning tuning! There is also a wonderful method called `timer` that can be used to time some part of code! 

- Then we move onto some Joins! The book covered 2 different join methods and give an example to optimize one of them. In [BroadcastHashJoin](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/BroadcastHashJoin.scala) we will generate some Dataframes on the fly and try to understand why this joining is feasible. In [SortMergeJoin](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/SortMergeJoin.scala) we will inspect the UI and detect an **Exchange** that we can get rid of with buckets! That example is in [SortMergeJoinBucketed](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/SortMergeJoinBucketed.scala).

- Secret - There is an application about MapAndMapPartitions in the books github page, but it is not in the book. [Here is our implementation](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/MapAndMapPartitions.scala), we will see the effect of opening and closing a FileWriter! Again, feel free to use the `benchmark` method in your applications!

#### Chapter 8 - Structured Streaming

- In Chapter 8, we will see the basics of Spark's Structured streaming. In [WordCount](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCount.scala) we will have an apllication up an running, and in [NumberCount](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/NumberCount.scala) we discover the value of `try/catch` blocks in the code, while working with Dataset API. The test [WordCountTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/WordCountTest.scala) will give us a simple testing blueprint for streaming applications!

- We can also write a [CustomListener](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/CustomListener.scala) to get more information about our `StreamingQuery`. We also have a method called `printProgress` that prints the `query.lastProgress` in a seperate Thread with defined periods, which might be useful.

- You will most likely work with Apache Kafka in StructuredStreaming, so even though there is not a complete example in the book, we have [WordCountKafka](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountKafka.scala) which will show us the basic `source -> process -> sink` pipeline. To be able to run it, we will use docker, as [explained in the readme.](https://github.com/kantarcise/learningspark/blob/main/docker/localSparkDockerKafka/readme.md)

- **You found a gift!**: Using the same docker containers, if you want to understand how you can communicate a streaming Dataframe between spark applications, you can check out [NestKafkaProducer](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/NestKafkaProducer.scala) and [NestLogsConsumer](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/NestLogsConsumer.scala). After that, If you are really curious, you can check out [SensorProducer](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/SensorProducer.scala) and [SensorConsumer](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/SensorConsumer.scala) to stream multiple Dataframes with Kafka and consume them in another Spark application just to join them! 

- What if we wanted to read and/or write to storage systems that do not have built-in support in Structured Streaming? In [WordCountToCassandra](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountToCassandra.scala) we will see how we can write the result of a streaming query to a Cassandra Instance. Again, we will use docker-compose for it, and the details are in the [readme file](https://github.com/kantarcise/learningspark/blob/main/docker/localSparkDockerCassandra/readme.md). This example will help us understand the two operations that allow us to write the output of a streaming query to arbitrary storage systems: `foreachBatch()` and `foreach()`. This was the example of `foreachBatch()`.

- What is another cool thing we can do with `foreachBatch()`? In [WordCountToFile](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountKafka.scala) we weill see how we can use `foreachBatch()` to save streaming Datasets into files. This is also a great example to understand `coalesce(1)`, we use it to gather all the words and counts into single Dataset! We test our approach in [WordCountToFileTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/WordCountToFileTest.scala)

- How about a PostgreSQL sink? In [WordCountToPostgres](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountToPostgres.scala) we will have the same example, with a PostgreSQL sink and we will discover how we can use `foreach()` for the streaming query! We will use [CustomForeachWriter](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/CustomForeachWriter.scala) to express the data-writing logic by dividing it into three methods: `open()`, `process()`, and `close()`. For docker setup and running the application, refer to [readme file](https://github.com/kantarcise/learningspark/blob/main/docker/localSparkDockerPostgreSQL/readme.md).

- There are a lot of data transformations we can use while doing structured streaming. In [StatelessOperations](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/StatelessOperations.scala) we will see how some of the data transformations process each input record individually without needing any information from previous rows. So the are called **Stateless!** Also, the application has a use case of `MemoryStream` to generate data on fly and make a streaming Dataset with it, check out `addDataPeriodicallyToMemoryStream(memoryStream, interval)` method! We will also try to understand `.outputMode("")` and how it works in stateless aggregations. For more information check out [16th and 17th items in Extras](https://github.com/kantarcise/learningspark?tab=readme-ov-file#extras).

- Stateful Operations are divided into two groups (Managed and Unmanaged - page 237). 

- **Managed Stateful Operations** are divided into three -> Streaming aggregations, Stream–stream joins, Streaming deduplication. We will start with Streaming aggregations:

    - Streaming Aggretations:

    - ***First***, we try to understand managed stateful operations that does not use **Time** in [ManagedStatefulAggregationsWithoutTime](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/ManagedStatefulAggregationsWithoutTime.scala) which uses a `MemoryStream` to generate a stream and shows us a lot of different types of aggregations in Structured Streaming. `allTypesOfAggregations(df: Dataframe)` is a great place to discover all kinds of aggregations. 

    - ***Second***, we will see stateful operations which **do** depend on time, with a tumbling and sliding window in [ManagedStatefulAggregationsWithTime](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/ManagedStatefulAggregationsWithTime.scala). Again, we are using `MemoryStream` to simulate a real stream, and we are working with case classes [SimpleSensorWithTime](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/SimpleSensorWithTime.scala) and [SimpleSensorWithTimestamp](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/SimpleSensorWithTimestamp.scala). We do a simple timestamp conversion too; with `map()` we convert a type of Dataset into a different Dataset! 

    - ***Third*** we will discover how we can prevent unbounded state, with [Watermarks](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/Watermarks.scala)! We will see how using a watermark effects the streaming Dataframe, with `numRowsDroppedByWatermark`.  We also have an example, [WatermarksWithSocket](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WatermarksWithSocket.scala) where our incoming data is from a socket - we use `.format("socket")` to make a streaming Dataframe! You can also play around with that!

    - Streaming Joins:

    - We will discover how Spark joins a static Dataframe/Dataset with a streaming one! In [StreamStaticJoinsDataset](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/StreamStaticJoinsDataset.scala) we will join a static Dataset with a streaming one and see the results! We will do the same in [StreamStaticJoinsDataframe](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/StreamStaticJoinsDataframe.scala) for Dataframes! We will understand the simple but important distinction between `join()` and `joinWith()`, as they are part of Dataframe/Dataset API (Untyped Transformation / Typed Transformation).We can also discover how to test such application, in [StreamStaticJoinsDataframeTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/StreamStaticJoinsDataframeTest.scala)

    - Our approach is clearly not suitable when both sources of data are changing rapidly. For that we need stream–stream joins, and we see our first example in [StreamStreamJoins](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/StreamStreamJoins.scala) In this application, we will explore what types of joins (inner, outer, etc.) are supported, and how to use watermarks to limit the state stored for stateful joins. We will test out `leftouter` joining with [StreamStreamJoinsTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/StreamStreamJoinsTest.scala)

    - Streaming deduplication:

    - We can deduplicate records in data streams using a unique identifier in the events. This is exactly same as deduplication on static using a unique identifier column. In [StreamingDeduplication](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/StreamingDeduplication.scala) we will see how a `guid` can be dropped when it's repeated in the upcoming batches, for a streaming Dataframe. There is also deduplication using both the `guid` and the `eventTime` columns with a watermark which bounds the amount of the state the query has to maintain. For more information, check out the [related part in Structed Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#streaming-deduplication). 

- **Unmanaged stateful operations** are where we define our own custom state cleanup logic.

    - We will start with a simple example with `mapGroupsWithState()` to illustrate the four key steps to modeling custom state data and defining custom operations on it. In [UnmanagedStatefulOperations](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/UnmanagedStatefulOperations.scala) we will update the state ourselves (with the help of `updateUserStatus()` which follows the `arbitraryStateUpdateFunction()` signature (page 254)), and test out approach in [UnmanagedStatefulOperationsTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/UnmanagedStatefulOperationsTest.scala). 
    
    - We move onto timeouts, and how we can use them to expire state that has not been updated for a while. First [UnmanagedStateProcessingTimeTimeout](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/UnmanagedStatefulOperations.scala) will show us how we can use ProcessingTime for out timeouts, in a similar structure with `UnmanagedStatefulOperations`. Because we are using the `case classes` with same name (`UserAction`, `UserStatus`), we will see how we can scope them into our object. Overall, this type of timeouts are easy to undertand, but there are a lot of downsides for using them. Check out [25th item in Extras](https://github.com/kantarcise/learningspark?tab=readme-ov-file#extras) for more information.

    - In [UnmanagedStateEventTimeTimeout](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/UnmanagedStateEventTimeTimeout.scala) we will see how **Event Time** is used for timeouts. The code looks mostly the same as `UnmanagedStateProcessingTimeTimeout`, it is a lot cleaner an there are great advantages! We test our approach in [UnmanagedStateEventTimeTimeoutTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/UnmanagedStateEventTimeTimeoutTest.scala) thanks to `org.scalatest.concurrent.Eventually`. For more information, check out [26th item in Extras](https://github.com/kantarcise/learningspark?tab=readme-ov-file#extras).

    - `flatMapGroupsWithState()`, gives us even more flexibility than `mapGroupsWithState()`. In [UnmanagedStateWithFlatMapGroupsWithState](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/UnmanagedStateWithFlatMapGroupsWithState.scala) we will discover how we can use `flatMapGroupsWithState()` in a similar fashion with the example applications we did before! We will make use of `org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}`, especially `OutputMode`.


#### Chapter 9 - Building Reliable Data Lakes with Apache Spark 🐢 

- Expressing the processing logic only solves half of the end-to-end problem of building a pipeline. Our goal is to build pipelines so that we can query the processed data and get insights from it. The choice of storage solution determines the end-to-end (i.e., from raw data to insights) robustness and performance of the data pipeline.

- Here is the short history:

    - **Ideal Storage**: An ideal Storage solution should be scalable and performent, supports ACID transactions, supports diverse data formats, supports diverse workloads and should be open.

    - **Databases?**: Databases was the most reliable solution for decades, with their strict schema and SQL queries. SQL workloads are divided into 2, **Online transaction processing (OLTP) workloads** (Like bank account transactions, high-concurrency, low-latency, simple queries) and **Online analytical processing (OLAP)** (like periodic reporting, complex queries (aggregates and joins), require high-throughput scans over many records).

    - **Spark Designed for ? 🤔**: Spark is primarily designed for **OLAP**.

    - **Limitations of Databases**: Growth in data size (advent of big data, global trend to measure and collect everything), Growth in the diversity of analytics (a need for deeper insights, ML - DL).

    - **Issues with Databases**: Databases are extremely expensive to scale out and Databases do not support non–SQL based analytics very well.

    - **Data Lakes**:  In contrast to most databases, a data lake is a distributed storage solution that runs on commodity hardware and easily scales out horizontally. The data lake architecture, unlike that of databases, decouples the distributed storage system from the distributed compute system. This allows each system to scale out as needed by the workload. Furthermore, the data is saved as files with open formats, such that any processing engine can read and write them using standard APIs.

    - **How to build as Datalake?**:  Organizations build their data lakes by independently choosing the following: **Storage system** ([HDFS](https://www.databricks.com/glossary/hadoop-distributed-file-system-hdfs) on cluster of machines or S3, Azure Data Lake Storage or GFS), **File format** (Depending on the downstream workloads, the data is stored as files in either structured (e.g., Parquet, ORC), semi-structured (e.g., JSON), or sometimes even unstructured formats (e.g., text, images, audio, video).), **Processing engine**(s) (depending on the workload, batch processing engine (Spark, Presto, Apache Hive), a stream processing engine (Spark, Apache Flink), or a machine learning library (e.g., Spark MLlib, scikit-learn, R)).

    - **The advantage of Data Lakes?**: The flexibility (the ability to choose the storage system, open data format, and processing engine that are best suited to the workload at hand) is the biggest advantage of data lakes over databases.

    - **Spark is great with Datalakes, why?**: Spark Support for diverse workloads, support for diverse file formats, support for diverse filesystems.

    - **What is the downside of Datalakes?**: Data lakes are not without their share of flaws, the most egregious of which is the lack of transactional guarantees: **Atomicity and isolation** Processing engines write data in data lakes as many files in a distributed manner. If the operation fails, there is *no mechanism to roll back* the files already written, thus leaving behind potentially corrupted data (the problem is exacerbated when concurrent workloads modify the data because it is very difficult to provide isolation across files without higher-level mechanisms), **Consistency** Lack of atomicity on failed writes further causes readers to get an inconsistent view of the data.

    - **Is there a better way? 🤔**: Attempts to eliminate such practical issues have led to the development of new systems, such as lakehouses.

    - **Lakehouses: The Next Step 🎉**: **Combines the best** elements **of data lakes** and **data warehouses** for OLAP workloads. 

        - **1️⃣ Transaction support**: Similar to Databases, **ACID** guarantees in concurrent workloads.

        - **2️⃣ Schema enforcement and governance**: Lakehouses prevent data with an incorrect schema being inserted into a table, and when needed, the table schema can be explicitly evolved to accommodate ever-changing data.

        - **3️⃣ Support for diverse data types in open formats**: Unlike databases, but similar to data lakes, lakehouses can store, refine, analyze, and access **all types of data** needed for many new data applications, be it structured, semi-structured, or unstructured. To enable a wide variety of tools to access it directly and efficiently, the data must be stored in open formats with standardized APIs to read and write them.

        - **4️⃣ Support for diverse workloads** Powered by the variety of tools reading data using open APIs, lakehouses enable diverse workloads to operate on data in a single repository. Breaking down isolated data silos (i.e., multiple repositories for different categories of data) enables developers to more easily build diverse and complex data solutions, from traditional SQL and streaming analytics to machine learning.
        
        - **5️⃣ Support for upserts and deletes**: Complex use cases like *change-data-capture* (CDC) and *slowly changing dimension* (SCD) operations require data in tables to be continuously updated. Lakehouses allow data to be concurrently deleted and updated with transactional guarantees.

        - **6️⃣ Data governance**: Lakehouses provide the tools with which you can reason about **data integrity** and audit all the data changes for policy compliance.

    - **Current selection of Lakehouses?**: Currently, there are a few open source systems, such as **Apache Hudi**, **Apache Iceberg**, and **Delta Lake**, that can be used to build lakehouses with these properties (more information page 272). 

- We focus on **Delta Lake**! It is hosted by the Linux Foundation, built by the original creators of Apache Spark. It is called Delta Lake because of its analogy to streaming. Streams flow into the sea to create deltas—this is where all of the sediments accumulate, and thus where the valuable crops are grown. Jules S. Damji (one of our coauthors) came up with this!

- To build a lakehouse, we need to configure Apache Spark to link to the Delta Lake Library. We can either provide the package with cli as we are using `spark-shell` or submitting an application like `--packages io.delta:delta-spark_2.12:jar:3.2.0` or we can add the dependency in our `build.sbt` file, like `libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0"`

> [!IMPORTANT]
> Please note that the Delta Lake on Spark Maven artifact has been renamed from `delta-core` (before 3.0) to `delta-spark` (3.0 and above). Because we are using Spark 3.5.0, we will use `delta-spark` 🥰 

- In [LoansStaticToDeltaLake](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/LoansStaticToDeltaLake.scala) we will see a simple example of how we can load a static data into Delta Lake and query from the view we've made.

- As with static DataFrames, we can easily modify our existing Structured Streaming jobs to write to and read from a Delta Lake table by setting the format to **"delta"**. [LoansStreamingToDeltaLake](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/LoansStreamingToDeltaLake.scala) will help us understand making a streaming Dataframe and writing the data into Delta Lake. We will use two different `MemoryStream[LoanStatus]` and write into the same table. After the write process, we will read the data back and see it in the console! Also, with [DeltaLakeACIDTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/DeltaLakeACIDTest.scala) we will test the ACID guarantees Delta Lake provides, with a small scale.

- In [LoansStaticAndStreamingToDeltaLake](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/LoansStaticAndStreamingToDeltaLake.scala) we will combine both static and streaming Dataset writes into a single DeltaLake table. This shows that we pretty much can do it all (static - static & stream - stream & stream)!

- [DeltaLakeEnforceAndEvolveSchema](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/DeltaLakeEnforceAndEvolveSchema.scala) will demonstrate the Delta Lake's ability to enforce or merge a schema when we are working with a Dataframe.

- [DeltaLakeTransformData](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/DeltaLakeTransformData.scala) is to show us variety of transformations that we can use on data.

- [DeltaLakeTimeTravel](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/DeltaLakeTimeTravel.scala) this example is from outside of the book, just to demonstrate all the cool things that we can do!


#### Chapter 10 - Machine Learning with MLlib 🎩

- To get to work for understanding Machine Learning in Spark, we encounter the first step of ML systems, which is Data Ingestion and Exploration. We have a data called [sf-airbnb-clean.parquet](https://github.com/kantarcise/learningspark/blob/main/data/sf-airbnb-clean.parquet) and this data is slightly preprocessed to remove outliers (e.g., Airbnbs posted for $0/night), converted all integers to doubles, and selected an informative subset of the more than one hundred fields. Further, for any missing numerical values in our data columns, we have imputed the median value and added an indicator column (the column name followed by `_na`, such as `bedrooms_na`). This way the ML model or human analyst can interpret any value in that column as an imputed value, not a true value. We take a peek at this data in [AirbnbExplorePreprocessedData](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/AirbnbExplorePreprocessedData.scala), feel free to work on it as you please.

- If you wanted to understand how data cleansing is made and want to see an example, you can check out [AirbnbDataCleansing](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/AirbnbDataCleansing.scala).

- We will start our journey of setting up Machine Learning Pipelines with a small step, in [AirbnbPricePredictionSimple](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/AirbnbPricePredictionSimple.scala) we will see how we can setup an incredibly simple `LinearRegression` pipeline. We will make a train/test split from our cleansed data and train a model after. We will see how we can use `VectorAssembler()` to put features into vectors, with `prepareFeatures()`.

- [AirbnbPricePredictionIntermediate](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/AirbnbPricePredictionIntermediate.scala) will help us understand log based prediction for categories like `price`, using `R Formula` and `save/load` for models so that we have Reusability. Checking out the methods `modelWithOneHotEncoding` and `betterModelWithLogScale` might be really helpful. Of course, we will need a measurement metric for deciding the performance of a model, and `evaluateModelRMSE()` & `evaluateModelR2()` will help us there.

- When we use RMSE for evaluating the performance of a model, we need a baseline. A simple baseline example is given in [AirbnbBaselineModel](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/AirbnbPricePredictionIntermediate.scala).

- So far we've worked with, *Linear Regression*. We could do some more hyperparameter tuning with the linear regression model, but we're going to try tree based methods and see if our performance improves. We used `OHE` for categorical features in `LinearRegression`. However, for decision trees, and in particular, ***random forests***, we should not `OHE` our variables. [AirbnbPricePredictDecisionTree](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/AirbnbPricePredictDecisionTree.scala) is the example where we attempt to use `DecisionTreeRegressor`.

- To understand the basics of *Hyperparameter Tuning*, we will discover more about tree based models in [AirbnbPricePredictionRandomForests](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/AirbnbPricePredictDecisionTree.scala). In which we will use a `CrossValidator` and `ParamGridBuilder`. We will also see how we can do some simple visualization for CrossValidatorModels as well as PipelineModels in `visualizeModel()` and `visualizePipelineModel()`.

#### Chapter 11 - Managing, Deploying, and Scaling Machine Learning Pipelines with Apache Spark 🎷

- In the previous chapter, we learned to build ML pipelines with MLlib. This chapter focuses on managing and deploying models. We'll learn to use MLflow to track, reproduce, and deploy MLlib models, understand the challenges and trade-offs of different deployment strategies, and design scalable ML solutions.

- To be able to run the following applications, you can use a docker container! 🐤 For details and step by step tutorial, see [the readme](https://github.com/kantarcise/learningspark/blob/main/docker/localSparkDockerMlflow/readme.md)

- With [MlflowSimple](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/MlflowSimple.scala) we will discover how we can use MLFlow to track and monitor our deployments for our ML Applications in Spark.

- With [MlflowWithContext](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/MlflowWithContext.scala) we can log tags, metrics and artifacts with using the `ActiveRun` only! Also, we will keep using the same experiment and submit new runs as we rerun the application!

- An extra example that is not in the book but in the [dbc, Chapter 11](https://github.com/databricks/LearningSparkV2/tree/master/notebooks) (To open the dbc, make a Databricks Community Edition Account and import the dbc into your workspace.) shows us how we can use `XGBoost` with Spark! In [AirbnbPricePredictionXGBoost](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/AirbnbPricePredictionXGBoost.scala) we will simply make a pipeline with `XGBoostRegressor` from `import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressor`. We need an addition in our `build.sbt` for this external library!

- Also, why not use a `CrossValidator` to find a better performing model? [AirbnbPricePredictionXGBoostCrossValidated](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/AirbnbPricePredictionXGBoostCrossValidated.scala) will help us perform cross validation and save the best model! 

- **Model Deployment Options with MLlib**: Deploying machine learning models means something different for every organization and use case. Business constraints will impose different requirements for latency, throughput, cost, etc., which dictate which mode of model deployment is suitable for the task at hand—be it batch, streaming, real-time 

|                         | Throughput | Latency Usage               | Example application       | 
|-------------------------|------------|-----------------------------|---------------------------| 
| Batch                   | High       | High (hours to days)        | Customer churn prediction | 
| Streaming               | Medium     | Medium (seconds to minutes) | Dynamic pricing           |
| Real Time               | Low        | Low (milliseconds)          | Online ad bidding         | 

- Maybe we can add the image here!

- **Batch**: We have already seen this in Chapter 10! Batch deployments represent the majority of use cases for deploying machine learning models, and this is arguably the easiest option to implement. You will run a regular job to generate predictions, and save the results to a table, database, data lake, etc. for downstream consumption. 3 important questions:

    - **How frequently will you generate predictions?**: There is a trade-off between latency and throughput. You will get higher throughput batching many predictions together, but then the time it takes to receive any individual predictions will be much longer, delaying your ability to  act on these predictions.

    - **How often will you retrain the model?**: Unlike libraries like sklearn or TensorFlow, MLlib does not support online updates or warm starts. If you’d like to retrain your model to incorporate the latest data, you’ll have to retrain the entire model from scratch, rather than getting to leverage the existing parameters. In terms of the frequency of retraining, some people will set up a regular job to retrain the model (e.g., once a month), while others will actively monitor the model drift to identify when they need to retrain.

    - **How will you version the model?**: You can use the MLflow Model Registry to keep track of the models you are using and control how they are transitioned to/from staging, production, and archived

- **Streaming**: Instead of waiting for an hourly or nightly job to process your data and generate predictions, Structured Streaming can continuously perform inference on incoming data. While this approach is more costly than a batch solution as you have to continually pay for compute time (and get lower throughput), you get the added benefit of generating predictions more frequently so you can act on them sooner. Streaming solutions in general are more complicated to maintain and monitor than batch solutions, but they offer lower latency.

- In [AirbnbPricePredictionStreaming](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/AirbnbPricePredictionStreaming.scala) we will simulate a streaming data streaming in a directory of Parquet files. We will use a model that we saved in [AirbnbPricePredictionRandomForests](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/AirbnbPricePredictDecisionTree.scala) and see how our model performs in a Streaming Application!

- **Near Real-Time**: If your use case requires predictions on the order of hundreds of milliseconds to seconds, you could build a prediction server that uses MLlib to generate the predictions. While this is not an ideal use case for Spark because you are processing very small  amounts of data, you’ll get lower latency than with streaming or batch solutions.

- **Real Time**: There are some domains where real-time inference is required, including fraud detection, ad recommendation, and the like. While making predictions with a small number of records may achieve the low latency required for real-time inference, you will need to contend with load balancing (handling many concurrent requests) as well as geolocation in latency-critical tasks.

- There are a few open source libraries, such as MLeap and ONNX, that can help you automatically export a supported subset of the MLlib models to remove their dependency on Spark.

- [ONNX](https://onnx.ai/) and [SynapseML](https://microsoft.github.io/SynapseML/) is good resources to read about here.

- Instead of exporting MLlib models, there are other third-party libraries that integrate with Spark that are convenient to deploy in real-time scenarios, such as XGBoost and H2O.ai’s Sparkling Water (whose name is derived from a combination of H2O and Spark)

- Although XGBoost is not technically part of MLlib, the [XGBoost4J-Spark library](https://xgboost.readthedocs.io/en/latest/jvm/xgboost4j_spark_tutorial.html) allows you to integrate distributed XGBoost into your MLlib pipelines. A benefit of XGBoost is the ease of deployment: after you train your MLlib pipeline, you can extract the XGBoost model and save it as a non-Spark model for serving in Python.

- **How can we leverage Spark for non-MLlib models?**: A common use case is to build a scikit-learn or TensorFlow model on a single machine, perhaps on a subset of your data, but perform distributed inference on the entire data set using Spark.

TIP: If the workers cached the model weights after loading it for the first time, subsequent calls of the same UDF with the same model loading will become significantly faster. For more information, check page 337.

- **Distributed Hyperparameter Tuning**: Even if you do not intend to do distributed inference or do not need MLlib’s distributed training capabilities, you can still leverage Spark for distributed hyperparameter tuning. `joblib` and `Hyperopt`, check out page 338.

- TODO: As an extra, we implemented an example [TaxiFarePredictionXGBoostGPU]() inspired from [gpu-accelerated-xgboost](https://www.nvidia.com/en-us/ai-data-science/spark-ebook/tutorial-gpu-accelerated-xgboost/). 

#### Chapter 12 - Epilogue: Apache Spark 3.0

- At the time of the writing of the book, Spark 3.0 was new. The bug fixes and feature enhancements are numerous, so for brevity, we will highlight just a selection of the notable changes and features pertaining to Spark components.

- `Dynamic Partition Pruning` - page 343. `Adaptive Query Execution` - page 345. `SQL Join Hints` - page 348. `Catalog Plugin API and DataSourceV2` - page 349. `Structured Streaming` - page 352. `PySpark, Pandas UDFs, and Pandas Function APIs` - page 354. `DataFrame and SQL Explain Commands` - page 358.

- **Really simply, what is AQE?**: Adaptive Query Execution (AQE) is query re-optimization that occurs during query execution based on runtime statistics. AQE in Spark 3.0 includes 3 main features: Dynamically coalescing shuffle partitions, Dynamically switching join strategies, Dynamically optimizing skew joins.

- TODO: To see how `Dataframe.explain()` method works from Spark 3.0 onwards, you can check out [DataframeExplainModes](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/DataframeExplainModes.scala).

- ### Use as Template 💭

If you simply want to use this repository as a template, here is the fastest way to do so.

- Install everything needed to develop Spark Applications. 

- Follow the New Project setup.  

- Depending on your data, problem and deployment, write away!

## Extras

1) Is there a Function Documentation that we can use?: Solution - [Sure, there it is.](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html)

2) How can I understand Spark's capability on Column processing ? - Solution: Here is [the link for documentation](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html), This is where the magic happens.

3) Is using [`expr()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.expr.html) bad? - Solution - If Dataframe API has what you are looking for , I think you should use that instead. Here is [an opinion.](https://stackoverflow.com/a/73645401)

4) Is there a better way to mock data? - Solution: Sure, there are [better alternatives](https://www.mockaroo.com/) than manually writing data.

5) Can we clean up the code written in Dataframe API, using the Dataset API? - Solution: Yes! Depending on your definition of cleaning up, Dataset API can be pretty neat. Take a look at [EXAMPLE HERE]

6) Is there a simple source that explains Repartition & Coalesce in Spark ? - Solution: This is an [attempt.](https://medium.com/@amitjoshi7/repartition-coalesce-in-apache-spark-76eb6203c316#:~:text=Use%20Coalesce%20when%20decreasing%20the%20number%20of%20partitions%20to%20reduce%20computational%20overhead.)

7) How about Data Partitioning? - Solution: Here is another [link.](https://medium.com/@dipayandev/everything-you-need-to-understand-data-partitioning-in-spark-487d4be63b9c)

8) How to submit applications ? - Solution - Official [docs are helpful.](https://spark.apache.org/docs/latest/submitting-applications.html)

9) How to submit a Spark job, written in Scala - Solution: Although a different deployment, this is a [useful link.](https://guide.ncloud-docs.com/docs/en/hadoop-vpc-16)

10) Can I get a Docker compose example - Solution  - Here [is one](https://github.com/bitnami/containers/blob/main/bitnami/spark/docker-compose.yml)

11) Deploy mode - Client vs Cluster ? - Solution - An [explaination](https://stackoverflow.com/a/28808269)

12) Can I get an overview about cluster mode ? - Solution - Yes for sure, [here](https://spark.apache.org/docs/latest/cluster-overview.html)

13) How does `--master` selection work when submitting or writing Spark applications, is there a hierachy? - Solution - Yes there is. Apllication overrides console, console overrides `conf` - [Link from Stack Overflow](https://stackoverflow.com/a/54599477)

14) What should I do If I wanted to use `sbt assembly` ? - Solution : Just use plugins.sbt to setup for assembly - [PROJECT STRUCTURE](https://stackoverflow.com/a/36186315) - add assembly to plugins.

15) What if there is a merging error when I try to run `sbt clean assembly`? - Solution: Here is the assembly [merge strategy](https://stackoverflow.com/a/39058507)

16) There are 3 output modes Structured Streaming Provides (page 212 in the book):

    - **Append Mode**: Only the new rows appended to the result table since the last trigger will be written to the external storage. This is applicable only in queries where existing rows in the result table cannot change (e.g., a map on an input stream).

    - **Complete Mode**: The entire updated result table will be written to external storage.

    - **Update Mode**: Only the rows that were updated in the result table since the last trigger will be changed in the external storage. This mode works for output sinks that can be updated in place, such as a MySQL table.

17) Here are 3 output modes of Structured Streaming, in detail (page 216 in the book):

    - **Append Mode**:  This is the default mode, where only the new rows added to the result table/DataFrame (for example, the counts table) since the last trigger will be output to the sink. Semantically, this mode guarantees that any row that is output is never going to be changed or updated by the query in the future. Hence, append mode is supported by only those queries (e.g., stateless queries) that will never modify previously output data. In contrast, our [word count query](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCount.scala) can update previously generated counts; therefore, it does not support append mode.

    - **Complete Mode**: In this mode, all the rows of the result table/DataFrame will be output at the end of every trigger. This is supported by queries where the result table is likely to be much smaller than the input data and therefore can feasibly be retained in memory. For example, our word count query supports complete mode because the counts data is likely to be far smaller than the input data.

    - **Update Mode**: In this mode, only the rows of the result table/DataFrame that were updated since the last trigger will be output at the end of every trigger. This is in contrast to append mode, as the output rows may be modified by the query and output again in the future. Most queries support update mode.

- Complete details on the output modes supported by different queries can be found in the latest [Structured Streaming Programming Guide.](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

18) **Semantic Guarantees with Watermark**: A watermark of 10 minutes guarantees that the engine will never drop any data that is delayed by less than 10 minutes compared to the latest event time seen in the input data. However, the guarantee is strict only in one direction. Data delayed by more than 10 minutes is not guaranteed to be dropped—that is, it may get aggregated.

19) Unlike streaming aggregations not involving time, aggregations with time windows can use **all three output modes** (page 245 in the book):

    - **Update mode**: In this mode, every micro-batch will output only the rows where the aggregate got updated. This mode can be used with all types of aggregations. Specifically for time window aggregations, watermarking will ensure that the state will get cleaned up regularly. **This is the most useful and efficient mode** to run queries with streaming aggregations. However, you cannot use this mode to write aggregates to append-only streaming sinks, such as any file-based formats like Parquet and ORC (unless you use ***Delta Lake***, which we will discuss in the next chapter). ✨
    
    - **Complete mode**:  In this mode, every micro-batch will output all the updated aggregates, irrespective of their age or whether they contain changes. While this mode can be used on all types of aggregations, for time window aggregations, **using complete mode means state will not be cleaned up even if a watermark is specified.** 🐍 Outputting all aggregates requires all past state, and hence aggregation data must be preserved even if a watermark has been defined. Use this mode on time window aggregations with caution, as this can lead to an indefinite increase in state size and memory usage. 

    - **Append mode**:  This mode can be used only with aggregations on event-time windows and with watermarking enabled. Recall that append mode *does not allow previously output results to change*. For any aggregation without watermarks, every aggregate may be updated with any future data, and hence these cannot be output in append mode. Only when watermarking is enabled on aggregations on event-time windows does the query know when an aggregate is not going to update any further. Hence, instead of outputting the updated rows, append mode outputs each key and its final aggregate value only when the watermark ensures that the aggregate is not going to be updated again. **The advantage of this mode** is that it allows you to write aggregates to **append-only streaming sinks** (e.g., files). ⛵ The disadvantage is that the output will be delayed by the watermark duration—the query has to wait for the trailing watermark to exceed the time window of a key before its aggregate can be finalized.

20) What is the difference between `.format("csv").load(filePath)` and `.csv(filePath)` ? 🤔 - Both methods achieve the same result but in slightly different ways. `.format("csv").load(filePath)` uses the `DataFrameReader` API with more explicit control over the data source format. It allows for more customization and can be more flexible in certain contexts. `.csv(filePath)` is a shorthand provided by the `DataFrameReader` API specifically for reading CSV files. It is more concise and generally preferred for simplicity.

21) Notes about Stream - Static Joins:

    - **Stateless 🐘**: Stream–static joins are stateless operations, and therefore do not require any kind of watermarking.

    - **Cache It! 🐱**: The static DataFrame is read repeatedly while joining with the streaming data of every micro-batch, so you can cache the static DataFrame to speed up the reads.

    - **You Gotta Restart If file changes 🎈**: If the underlying data in the data source on which the static DataFrame was defined changes, whether those changes are seen by the streaming query depends on the specific behavior of the data source. For example, if the static DataFrame was defined on files, then changes to those files (e.g., appends) will not be picked up until the streaming query is restarted.

22) A few key points to remember about **inner joins**: 

    - For inner joins, specifying watermarking and event-time constraints are both optional. In other words, at the risk of potentially unbounded state, you may choose not to specify them. Only when both are specified will you get state cleanup.

    - Similar to the guarantees provided by watermarking on aggregations, a watermark delay of two hours guarantees that the engine will never drop or not match any data that is less than two hours delayed, but data delayed by more than two hours may or may not get processed.

23) However, there are a few additional points to note about **outer joins**:

    - **Watermark and Event Time Constraint is a Must**: Unlike with inner joins, the watermark delay and event-time constraints are not optional for outer joins. This is because for generating the NULL results, the engine must know when an event is not going to match with anything else in the future. For correct outer join results and state cleanup, the watermarking and event-time constraints must be specified.

    - **There will be delays!** Consequently, the outer NULL results will be generated with a delay as the engine has to wait for a while to ensure that there neither were nor would be any matches. This delay is the maximum buffering time (with respect to event time) calculated by the engine for each event as discussed in the previous section (in our example, 25 minutes hours for impressions and 10 minutes for clicks).

24) Notes about `mapGroupsWithState()` (page 256):

    - **You might want to reorder**: When the function is called, there is no well-defined order for the input records in the new data iterator (e.g., newActions). If you need to update the state with the input records in a specific order (e.g., in the order the actions were performed), then you have to explicitly reorder them (e.g., based on the event timestamp or some other ordering ID). In fact, if there is a possibility that actions may be read out of order from the source, then you have to consider the possibility that a future micro-batch may receive data that should be processed before the data in the current batch. In that case, you have to buffer the records as part of the state.

    - **You cannot update/remove state for a ghost user**: In a micro-batch, the function is called on a key once only if the micro-batch has data for that key. For example, if a user becomes inactive and provides no new actions for a long time, then by default, the function will not be called for a long time. If you want to update or remove state based on a user’s inactivity over an extended period you have to use **timeouts**, which we will discuss in the next section.

    - **Cannot append to files**: The output of `mapGroupsWithState()` is assumed by the incremental processing engine to be continuously updated key/value records, similar to the output of aggregations. This limits what operations are supported in the query after mapGroupsWithState(), and what sinks are supported. For example, appending the output into files is not supported. If you want to apply arbitrary stateful operations with greater flexibility, then you have to use `flatMapGroupsWithState()`. We will discuss that after timeouts.


25) Here are a few points to note about **Processing Time Timeouts** (page 258):

    -  The timeout set by the last call to the function is automatically cancelled when the function is called again, either for the new received data or for the timeout. Hence, whenever the function is called, the timeout duration or timestamp needs to be *explicitly set* to enable the timeout.

    - **Imprecise, as you would guess**: Since the timeouts are processed during the micro-batches, the timing of their execution is imprecise and depends heavily on the trigger interval and micro-batch processing times. Therefore, it is not advised to use timeouts for precise timing control.

    - **I too like to live dangerously**: While processing-time timeouts are simple to reason about, they are not robust to slowdowns and downtimes. If the streaming query suffers a downtime of **more than one hour**, then after restart, all the **keys in the state will be timed out** because more than one hour has passed since each key received data. Similar wide-scale timeouts can occur if the query processes data slower than it is arriving at the source (e.g., if data is arriving and getting buffered in Kafka). For example, if the timeout is five minutes, then a sudden drop in processing rate (or spike in data arrival rate) that causes a five-minute lag could produce spurious timeouts. To avoid such issues we can use an event-time timeout.

26) Here are a few points to note about **Event Time Timeouts** (page 260):

    - **Why `GroupState.setTimeoutTimestamp()` ?**: Unlike in the previous example with processing-time timeouts, we have used `GroupState.setTimeoutTimestamp()` instead of `GroupState.setTimeoutDuration()`. This is because with processing-time timeouts the duration is sufficient to calculate the exact future timestamp (i.e., current system time + specified duration) when the timeout would occur, but this is not the case for event-time timeouts. Different applications may want to use different strategies to calculate the threshold timestamp. In this example we simply calculate it based on the current watermark, but a different application may instead choose to calculate a key’s timeout timestamp based on the maximum event-time timestamp seen for that key (tracked and saved as part of the state).

    - **Timeout Should Be Set Relative**: The timeout timestamp must be set to a value larger than the current watermark. This is because the timeout is expected to happen when the timestamp crosses the watermark, so it’s illogical to set the timestamp to a value already larger than the current watermark. (In our example, we did set the timeout timestamp as relative to the current watermark plus 1 hour (3600000 ms))


27) You can generate things other than fixed-duration timeouts. For example, you can implement an approximately periodic task (say, every hour) on the state by saving the last task execution timestamp in the state and using that to set the processing-time timeout duration, as shown in this code snippet:

```scala
// In Scala
timeoutDurationMs = lastTaskTimstampMs + periodIntervalMs -
    groupState.getCurrentProcessingTimeMs()
```

28) Generalization with `flatMapGroupsWithState()`. There are two key limitations with `mapGroupsWithState()` that may limit the flexibility that we want to implement more complex use cases (e.g., chained sessionizations):

    - **One Record At A Time 😕**: Every time `mapGroupsWithState()` is called, you have to return one and only one record. For some applications, in some triggers, you may not want to output anything at all.

    - **Engine Assumptions 😕**: With `mapGroupsWithState()`, due to the lack of more information about the opaque state update function, the engine assumes that generated records are updated key/value data pairs. Accordingly, it reasons about downstream operations and allows or disallows some of them. For example, the DataFrame generated using `mapGroupsWithState()` cannot be written out in append mode to files. However, some applications may want to generate records that can be considered as appends.
    
    - `flatMapGroupsWithState()` overcomes these limitations, at the cost of slightly more complex syntax. It has two differences from `mapGroupsWithState()`:

    - **An Iterator Instead of Object 🌠**: The return type is an iterator, instead of a single object. This allows the function to return any number of records, or, if needed, no records at all.

    - **We have an output mode 🍓**: It takes another parameter, called the operator output mode (not to be confused with the query output modes we discussed earlier in the chapter), that defines whether the output records are new records that can be appended (`OutputMode.Append`) or updated key/value records (`OutputMode.Update`).

29) **Performance Tuning** in Structured Streaming:

    - Unlike batch jobs that may process gigabytes to terabytes of data, micro-batch jobs usually process **much smaller volumes** of data. Hence, a Spark cluster running streaming queries usually needs to be tuned slightly differently. Here are a few considerations to keep in mind:

        -  **Cluster resource provisioning 🔎**:  Since Spark clusters running streaming queries are going to run 24/7, it is important to provision resources appropriately. Underprovisoning the resources can cause the streaming queries to fall behind (with micro-batches taking longer and longer), while overprovisioning (e.g., allocated but unused cores) can cause unnecessary costs. Furthermore, allocation should be done based on the nature of the streaming queries: **stateless queries usually need more cores, and stateful queries usually need more memory**.

        - **Number of partitions for shuffles** : For Structured Streaming queries, the number of shuffle partitions usually needs to be set **much lower** than for most batch queries—dividing the computation too much increases overheads and reduces throughput. Furthermore, shuffles due to stateful operations have significantly higher task overheads due to checkpointing. Hence, for **streaming queries with stateful operations** and trigger intervals of a few seconds to minutes, it is recommended to tune the **number of shuffle partitions** from the default value of **200** to at most **two to three times the number of allocated cores.**

        - **Setting source rate limits for stability 🪟**: After the allocated resources and configurations have been optimized for a query’s expected input data rates, it’s possible that sudden surges in data rates can generate unexpectedly large jobs and subsequent instability. Besides the costly approach of overprovisioning, you can safeguard against instability using source rate limits. Setting limits in supported sources (e.g., Kafka and files) prevents a query from consuming too much data in a single micro-batch. The surge data will stay buffered in the source, and the query will eventually catch up. However, note the following:
            - • Setting the limit too low can cause the query to underutilize allocated resources and fall behind the input rate.
            - • Limits do not effectively guard against sustained increases in input rate. While stability is maintained, the volume of buffered, unprocessed data will grow indefinitely at the source and so will the end-to-end latencies.

        - **Multiple streaming queries in the same Spark application**: Running multiple streaming queries in the same SparkContext or SparkSession can lead to fine-grained resource sharing. However:
            - • Executing each query continuously uses resources in the Spark driver (i.e., the JVM where it is running). This limits the number of queries that the driver can execute simultaneously. Hitting those limits can either bottleneck the task scheduling (i.e., underutilizing the executors) or exceed memory limits.
            - • You can ensure fairer resource allocation between queries in the same context by setting them to run in separate scheduler pools. Set the SparkContext’s thread-local property `spark.scheduler.pool` to a different string value for each stream:

```scala
// In Scala

// Run streaming query1 in scheduler pool1
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
df.writeStream.queryName("query1").format("parquet").start(path1)

// Run streaming query2 in scheduler pool2
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool2")
df.writeStream.queryName("query2").format("parquet").start(path2)
```

30) **Delta Lake Enforce / Merge:** The Delta Lake format records the schema as table-level metadata. Hence, all writes to a Delta Lake table can verify whether the data being written has a schema compatible with that of the table. If it is not compatible, Spark will throw an error before any data is written and committed to the table, thus preventing such accidental data corruption.


31) **Delta Lake Extended Merge Syntax:** A common use case when managing data is fixing errors in the data. Suppose, upon reviewing some data (that we work on, about Loans), we realized that all of the loans assigned to `addr_state = 'OR'` should have been assigned to `addr_state = 'WA'`. If the loan table were a Parquet table, then to do such an update we would need to:

    - Copy all non effected rows into new table.

    - Copy all effected into a Dataframe and perform modification.

    - Insert changed Dataframe's rows into new table.

    - Remove old table, rename new table to old table.

Delta lake is great and simple for this!

There are even more complex use cases, like CDC with deletes and SCD tables, that are made simple with the extended merge syntax. Refer to the [documentation](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge) for more details and examples.

32) **Querying Previous Snapshots in DeltaLake:** We can query previous versioned snapshots of a Delta Table. This is useful in a variety of situations, such as: 
    
    - Reproducing machine learning experiments and reports by rerunning the job on a specific table version 
    
    - Comparing the data changes between different versions for auditing 
    
    - Rolling back incorrect changes by reading a previous snapshot as a DataFrame and overwriting the table with it.

33) **What did we learn about DeltaLake? 🤔:** Databases have solved data problems for a long time, but they fail to fulfill the diverse requirements of modern use cases and workloads. Data lakes were built to alleviate some of the limitations of databases, and Apache Spark is one of the best tools to build them with. However, data lakes still lack some of the key features provided by databases (e.g., ACID guarantees). Lakehouses are the next generation of data solutions, which aim to provide the best features of databases and data lakes and meet all the requirements of diverse use cases and workloads.

Lakehouses (Deltalake in our case) provide:

    - Transactional guarantees and schema management, like databases

    - Scalability and openness, like data lakes

    - Support for concurrent batch and streaming workloads with ACID guarantees

    - Support for transformation of existing data using update, delete, and merge operations that ensure ACID guarantees

    - Support for versioning, auditing of operation history, and querying of previous versions


34) **Machine Learning with MLlib**:  Up until this point, we have focused on data engineering workloads with Apache Spark. Data engineering is often a precursory step to preparing your data for machine learning (ML) tasks. Chances are that whether we realize it or not, every day we come into contact with ML models for purposes such as online shopping recommendations and advertisements, fraud detection, classification, image recognition, pattern matching, and more. 

Building a model that performs well can make or break companies.

35) **What is Machine Learning, exactly?**: Machine learning is a process for extracting patterns from your data, using statistics, linear algebra, and numerical optimization. There are a few types of machine learning, including *supervised*, *semi supervised*, *unsupervised*, and *reinforcement learning*.

In supervised learning, the training data is labeled and the goal is to predict the output for an unlabeled input. The output can be discrete or continuous, which brings us two types, **classification** and **regression**. Classification example, is this picture of a dog or a cat? Regression example: predicting ice cream sales based on temperature.

36) **What is avaliable to us in Mllib? 🐱**

| Method | Typical Usage |
| ---- | ----- |  
| Linear regression | Regression |
| Logistic regression | Classification (although it has regression in it's name) |
| Decision Trees | Both |
| Gradient Boosted Trees | Both |
| Random Forests | Both |
| Naive Bayes | Classification |
| Support Vector Machines | Classification  |

To learn more, check out [Machine Learning Guide](https://spark.apache.org/docs/latest/ml-guide.html) from Spark. 

37) **Unsupervised Learning? What is unsupervised about it?**: Obtaining the labeled data required by supervised machine learning can be very expensive and/or infeasible. This is where unsupervised machine learning comes into play. Instead of predicting a label, unsupervised ML helps you to better understand the structure of your data. Unsupervised machine learning can be used for outlier detection or as a preprocessing step for supervised machine learning—for example, to reduce the dimensionality. Some unsupervised machine learning algorithms in MLlib include k-means, Latent Dirichlet Allocation (LDA), and Gaussian mixture models.

38) **PCA**: Here is [the video](https://www.youtube.com/watch?v=FgakZw6K1QQ) for more information. Which variable is the most valuable to clustering the data? When we get 4 variables we can no longer graph it to see resemblence between instances. For each variable we can acualte the averages, and shith the data so that this center is the origin in the graph (this did not change relative distances). We are looking for a line that crosses origin which seperates the the data best. We find the line with sum of squared distances to this candidate line, minimized. Depending on the slope of your axis, you can measure how data is spread out. (0.25, mostly spread on Math (x axis) a little spread out on Chemistry (y axis)).

In Spark PCA is RDD based so it is part of `spark.mllib`!

39) **`spark.ml`, `spark.mllib` ? Why is there 2 libraries?** `spark.mllib` is the original machine learning API, based on the RDD API (which has been in  maintenance mode since Spark 2.0), while spark.ml is the newer API, based on DataFrames. 

40) **Designing Machine Learning Pipelines:** Pipelines is a way to organize a series of operations to apply to our data! In `MLlib`, the Pipeline API provides a high-level API built on top of DataFrames to organize our machine learning workflow. Here are some common terminology:

    - **Transformer**: (Dataframe -> Dataframe) Accepts a DataFrame as input, and returns a new DataFrame with one or more columns appended to it. Transformers do not learn any parameters from your data and simply apply ***rule-based transformations*** to either ***prepare data for model training*** or ***generate predictions*** using a trained MLlib model. They have a `.transform()` method.

    - **Estimator**: Learns (or “fits”) parameters from your DataFrame via a `.fit()` method and returns a `Model`, which is a transformer. `LinearRegression` is an Estimator, some other examples of estimators include `Imputer`, `DecisionTreeClassifier`, and `RandomForestRegressor`. `lr.fit()` returns a `LinearRegressionModel` (lrModel), which is a transformer. In other words, the output of an estimator’s `fit()` method is a transformer.

    - **Pipeline**: Organizes a series of transformers and estimators into a single model. While pipelines themselves are estimators, the output of `pipeline.fit()` returns a `PipelineModel`, a transformer. Again, In Spark, `Pipelines` are **estimators**, whereas `PipelineModels` (fitted Pipelines) are **transformers**.


41) **One Hot Encoding with ``SparseVector``**: Most machine learning models in MLlib expect numerical values as input, represented as vectors. To convert categorical values into numeric values, we can use a technique called one-hot encoding (OHE). Suppose we have a column called Animal and we have three types of animals: Dog, Cat, and Fish. We can’t pass the string types into our ML model directly, so we need to assign a numeric mapping, such as this:
    
    - Animal = {"Dog", "Cat", "Fish"}
    
    - "Dog" = 1, "Cat" = 2, "Fish" = 3

However, using this approach we’ve introduced some spurious relationships into our data set that weren’t there before. For example, why did we assign Cat twice the value of Dog? The numeric values we use should not introduce any relationships into our data set. Instead, we want to create a separate column for every distinct value in our Animal column:
    
    - "Dog" = [ 1, 0, 0]
    
    - "Cat" = [ 0, 1, 0]
    
    - "Fish" = [0, 0, 1]

If the animal is a dog, it has a one in the first column and zeros elsewhere. If it is a cat, it has a one in the second column and zeros elsewhere.

If we had a zoo of 300 animals, would OHE massively increase consumption of memory/compute resources? Not with Spark! Spark internally uses a `SparseVector` when
the majority of the entries are 0, as is often the case after OHE, so it does not waste space storing 0 values.

There are a few ways to one-hot encode your data with Spark. A common approach is to use the `StringIndexer` and `OneHotEncoder`.

42) **Interpreting the value of RMSE.:** So how do we know if 220.6 is a good value for the RMSE? There are various ways to interpret this value, one of which is to build a simple baseline model and compute its RMSE to compare against. A common baseline model for regression tasks is to compute the average value of the label on the training set ȳ (pronounced y-bar), then predict ȳ for every record in the test data set and compute the resulting RMSE (example code is available in the book’s GitHub repo).


43) **Why is our first model not performing well?**  Our $R^2$ is positive, but it’s very close to 0. One of the reasons why our model is not performing too well is because our label, price, appears to be log-normally distributed. If a distribution is log-normal, it means that if we take the logarithm of the value, the result looks like a normal distribution. Price is often log-normally distributed. If you think about rental prices in San Francisco, most cost around $200 per night, but there are some that rent for thousands of dollars a night!

44) **Is there Warm Start in Spark ML?**: After you've saved a model to disk, and loaded it in another Spark application, you can apply it to new data points. However, you can’t use the weights from this model as initialization parameters for training a new model (as opposed to starting with random weights), as Spark has no concept of “warm starts.” If your data set changes slightly, you’ll have to retrain the entire linear regression model from scratch. 

45) **Decision Tree Hyperparameters?**: The depth of a decision tree is the longest path from the root node to any given leaf node. Trees that are very deep are prone to **overfitting**, or memorizing noise in your training data set, but trees that are too shallow will underfit to your data set (i.e., could have picked up more signal from the data).

46) **The Idea Behind Random Forests?** Ensembles work by taking a democratic approach. Imagine there are many M&Ms in a jar. You ask one hundred people to guess the number of M&Ms, and then take the average of all the guesses. The average is probably closer to the true value than most of the individual guesses. That same concept applies to machine learning models. If you build many models and combine/average their predictions, they will be more robust than those produced by any individual model. Random forests are an ensemble of decision trees. For more details, check out page 314 in the book.

47) **Choosing the Best Hyperparameters??**:  So how do we determine what the optimal number of trees in our random forest or the max depth of those trees should be? This process is called hyperparameter tuning. 

As an example, here are the steps to perform a hyperparameter search in Spark:

    1. Define the estimator you want to evaluate.

    2. Specify which hyperparameters you want to vary, as well as their respective values, using the ParamGridBuilder.

    3. Define an evaluator to specify which metric to use to compare the various models.

    4. Use the CrossValidator to perform cross-validation, evaluating each of the various models.

48) **Random Forests - Random feature selection by columns**: The main drawback with bagging is that the trees are all highly correlated, and thus learn similar patterns in your data. To mitigate this problem, each time you want to make a split you only consider a random subset of the columns (1/3 of the features for `RandomForestRegressor` and $(#features)^0.5$ for `RandomForestClassifier`). Due to this randomness you introduce, you typically want each tree to be quite shallow. You might be thinking: each of these trees will perform worse than any single decision tree, so how could this approach possibly be better? It turns out that **each of the trees learns something different about your data set**, and combining this collection of “weak” learners into an ensemble makes the forest much more robust than a single decision tree.

49) **Random Forests For Classification/Regression**:  If we build a random forest for classification, it passes the test point through each of the trees in the forest and takes a majority vote among the predictions of the individual trees. (By contrast, in regression, the random forest simply averages those predictions.) Even though each of these trees is less performant than any individual decision tree, the collection (or ensemble) actually provides a more robust model. Random forests truly demonstrate the power of distributed machine learning with Spark, as each tree can be built independently of the other trees (e.g., we do not need to build tree 3 before we build tree 10). Furthermore, within each level of the tree, you can parallelize the work to find the optimal splits.

50) **Optimizing Pipelines**: The value of parallelism should be chosen carefully to maximize parallelism without exceeding cluster resources, and larger values may not always lead to improved performance. Generally speaking, a value up to 10 should be sufficient for most clusters.

51) **Cross Validator Inside Pipeline or Vice Versa ??**:  There’s another trick we can use to speed up model training: putting the cross-validator inside the pipeline (e.g., `Pipeline(stages=[..., cv])`) instead of putting the pipeline inside the cross-validator (e.g., `CrossValidator(estimator=pipeline, ...)`). Every time the cross-validator evaluates the pipeline, it runs through every step of the pipeline for each model, even if some of the steps don’t change, such as the StringIndexer. By reevaluating every step in the pipeline, we are learning the same StringIndexer mapping over and over again, even though it’s not changing. If instead we put our cross-validator inside our pipeline, then we won’t be reevaluating the StringIndexer (or any other estimator) each time we try a different model.

52) **Logging - Configuring Logging Programmatically**: Spark gives us the ability to configure logging as we please. Since Spark 3.3, Spark migrates its log4j dependency from 1.x to 2.x, [here is the link](https://spark.apache.org/docs/latest/core-migration-guide.html#upgrading-from-core-32-to-33). We can configure the `log4j2.properties` file under our spark conf, which is typically at `/usr/local/spark/conf`, but for now, this is confusing for me. Main reason for this is that the `log4j2.properties` file effects all of the Spark. Instead, we can configure logging programmatically! To see an example of this, check out [MnmCandiesDatasetWithLogger](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/MnmCandiesDatasetWithLogger.scala).

## Offer

Maybe you've never coded in Java/Scala. You tried your luck with PySpark but it never went to being more than a small side project.

Well, with this resource, you will have a window for a [20 seconds of insane courage](https://www.youtube.com/watch?v=OFp2Rn5foIc).

After you covered all the material, you'll be confident in your ability to solve problems with this tool.

And will be able to dive deep if needed.