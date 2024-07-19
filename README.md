# learningspark

All the exercises from the book "[Learning Spark](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)", solved with Dataframes and Datasets, no stone unturned.

## Why ?

There are a lot of sources on Spark, like [LearningApacheSpark](https://github.com/runawayhorse001/LearningApacheSpark), [learning-spark](https://github.com/databricks/learning-spark) from Databricks and the [updated version](https://github.com/databricks/LearningSparkV2/tree/master). Sadly, they are old and not recently updated. 

You have to wander a lot just to get the ball rolling, especially if you are new to the environment.

I thought, it shouldn't be this hard.

## Scala ? 🤔 I don't know any Scala 😕

Focus for 90 minutes and finish the [Prelude꞉ A Taste of Scala](https://docs.scala-lang.org/overviews/scala-book/prelude-taste-of-scala.html) here. After that, if you still want to discover more, you can check out [Tour of Scala](https://docs.scala-lang.org/tour/tour-of-scala.html). You can test your understanding [in this website](https://scastie.scala-lang.org/).

## Usage 🔨

You have 3 choices:

### Instuction Route - Instructions 🎫 

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

Here are all the steps. If you get stuck, you can refer back to the video.

Prerequisites:

- Install Java 11 and add it to path. Wait, what was [JDK, JRE, JVM again ??](https://www.geeksforgeeks.org/differences-jdk-jre-jvm/)

- Install Scala 2.18.12 from the source and add it to path. Why [Scala ?](https://www.projectpro.io/article/why-learn-scala-programming-for-apache-spark/198)

- Install Sbt 1.9.6 and add it to path. What is [sbt ?](https://www.scala-sbt.org/1.x/docs/) 

- Install Spark 3.5.0, unzip it. Add it to path that you unzipped. With or Without Hadoop ? Here is [the answer.](https://stackoverflow.com/questions/48514247/if-i-already-have-hadoop-installed-should-i-download-apache-spark-with-hadoop-o)

Now you should be able to use 

```bash
spark-shell
```

and you are ready to roll!

2) For development we will use [Intellij IDEA](https://www.jetbrains.com/idea/). It has a 30 day trial period, so no worries.

Download Intellij IDEA and follow the `readme` inside compressed file:

Which is simply summarized as: 

- To start the application, open a console, cd into "{installation home}/bin" and type:

```bash
./idea.sh
```
To open the IDE. You can make a [Desktop Shortcut](https://askubuntu.com/a/1145563) and add it to your Favorites.

3) To setup a project from scratch, select `New Project`, select JDK, Scala and SBT versions that we installed, make the package prefix something that you choose.

4) Now you are ready to try out some code!

### Code Catalog 📚

Here is all the code explained in detail.

#### Chapter 0 - A Fast Start (Not a Part of the Book)

- If you want to just see what are Dataset's and Dataframe's in Spark, you can check out [Aggregates](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/Aggregates.scala). What is aggregates mean, you ask? Well, continue to find out. 

#### Chapter 1 - Introduction to Apache Spark: A Unified Analytics Engine

- This chapter is just a reading assignment. I think it is important to learn the history/roots of tools and also have a high level picture what they might look like, as we try to master them.

#### Chapter 2 - Downloading Apache Spark and Getting Started

- We will see our first example where we load some data onto Spark and tried to answer some questions based on the data! Processed sugar is terrible and you know it, however, here is an [example on MnM's](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/MnmCandies.scala) We will discover about relative paths, user inputs and Dataframe Schemas.

- Also here is the same example, using [only Datasets!](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/MnmCandiesDataset.scala) 

#### Chapter 3 - Apache Spark’s Structured APIs

- In Chapter 3 we will work with a JSON file about bloggers in Databricks. Let's see who is the most articulate, with [Bloggers](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/Bloggers.scala)!

- We will also have a Dataset API version, [here](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/BloggersDataset.scala). In here, we can discover about making a Dataframe from JUST ROWS - no case classes. We will see different ways of interacting with cols (`expr`, `$`, `col("someColumnName")`) and have a simple example of sorting & concatenating.

- After that, we will discover the about a FireCalls data in CSV format. This will be an opportunity to discover a common typecast between String to Timestamp. Here is our simple appetizer, [FireCalls](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FireCalls.scala) and [FireCallsDataset](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FireCallsDataset.scala). In the test of this object, we will see how to mock data with `Some`, explained here: [FireCallsTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/FireCallsTest.scala) 

- For our FireCalls CSV data, we have some questions that are solved in the book, so we will go over them to understand in [FireCallsSolvedQuestions](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FireCallsSolvedQuestions.scala) and [FireCallsSolvedQuestionsDataset](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FireCallsSolvedQuestionsDataset.scala). In these, we will see how to save our data as Parquet files & Tables, and some function usage like `distinct()`, `countDistinct()`, `withColumn`, `withColumnRenamed`, `lit()`, `datesub()` and `as()`. 

- Finally, for the FireCalls CSV data, we will solve all the training questions from the book. In [FireCallsTraningQuestions](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FireCallsTraningQuestions.scala) and [FireCallsTraningQuestionsDataset](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FireCallsTraningQuestionsDataset.scala) we have an end to end pipeline, using `to_timestamp`, `lit()` some columns ourselves, `timestamp - where().where().where()` or `timestamp - between()`, `weekofyear()`,  `saveAsTable()` and parallel writ9e to filesystem.

- Then we will get to the IoT Devices data, as a JSON. We will start with a basic workout on Datasets, discovering the differencee between Lambdas and DSL expressions (Page 170 in the book), with [IoTDatasetFunctionComparison](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/IoTDatasetFunctionComparison.scala).

- At the end of Chapter 3, we will practice our skills on the IoT Devices data and discover more with [IotDatasetWorkout](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/IotDatasetWorkout.scala) as an End to End pipeline. This will teach us about `collect`, `take`, `first`, `.stat.approxQuantile` and different types of combinations for `groupBy - agg - orderBy` and `groupBy - orderBy` on multiple cols. We will test all of our approaches, [here.](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/IotDatasetWorkoutTest.scala)

#### Chapter 4 - Spark SQL and DataFrames: Introduction to Built-in Data Sources

- In Chapter 4 we will discover about Spark SQL, which is literally SQL stuff. For that, we will use a data about Flight Delays, a CSV file. [FlightDelays](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FlightDelays.scala) we will discover more about, views & tables, union Dataframes, instead of using multiple unions, using `when().when().when().when().otherwise()`, `explain()` to see the plans, and a sneak peak to `freqItems` -> `df.stat.freqItems(Seq("origin"))` Also, the [Dataset version](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FlightDelaysDataset.scala) simply requires a case class like [Flights](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/Flights.scala)

- On mocked data, you can see our techniques tested in [FlightDelaysTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/FlightDelaysTest.scala)

#### Chapter 5 - Spark SQL and DataFrames: Interacting with External Data Sources

- In this chapter, we will again work on Flight Delay data, but we will discover different things! With [FlightDelaysAdvanced](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FlightDelaysAdvanced.scala) we will learn about making Tables - `createOrReplaceTempView`, getting rid of `expr()` and understanding it's use case (expr() function to use SQL syntax anywhere a column would be specified.). 

- We will see about casting types, like `.withColumn("delay", $"delay".cast(IntegerType))`, filtering multiple things like `filter($"a" === 1 && $"b" === 2)`, `.startsWith("")`, joins (inner join), `.isin("a", "b", "c")`, window functions. We will see functions like `dense_rank`, `drop - rename - pivot`. A different type cas is also waiting us, strings to time -> `02190925 - 02-19 09:25 am` , `.withColumn("month", $"date".substr(0, 2).cast(IntegerType))`.

- The main methods are getting really long at this point. If you want some help decomposing the code, you can see an examplem at [FlightDelaysAdvancedDecomposed](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/FlightDelaysAdvancedDecomposed.scala) Tests are at [FlightDelaysAdvancedTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/FlightDelaysAdvancedTest.scala)

- Finally, we will see a simple example to understand windowing, with [SimpleWindowing](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/SimpleWindowing.scala)

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

- What if we wanted to read and/or write to storage systems that do not have built-in support in Structured Streaming? In [WordCountToCassandra](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountToCassandra.scala) we will see how we can write the result of a streaming query to a Cassandra Instance. Again, we will use docker-compose for it, and the details are in the [readme file](https://github.com/kantarcise/learningspark/blob/main/docker/localSparkDockerCassandra/readme.md). This example will help us understand the two operations that allow us to write the output of a streaming query to arbitrary storage systems: `foreachBatch()` and `foreach()`. This was the example of `foreachBatch()`.

- What is another cool thing we can do with `foreachBatch()`? In [WordCountToFile](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountKafka.scala) we weill see how we can use `foreachBatch()` to save streaming Datasets into files. This is also a great example to understand `coalesce(1)`, we use it to gather all the words and counts into single Dataset! We test our approach in [WordCountToFileTest](https://github.com/kantarcise/learningspark/blob/main/src/test/scala/WordCountToFileTest.scala)

- How about a PostgreSQL sink? In [WordCountToPostgres](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountToPostgres.scala) we will have the same example, with a PostgreSQL sink and we will discover how we can use `foreach()` for the streaming query! We will use [CustomForeachWriter](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/CustomForeachWriter.scala) to express the data-writing logic by dividing it into three methods: `open()`, `process()`, and `close()`. For docker setup and running the application, refer to [readme file](https://github.com/kantarcise/learningspark/blob/main/docker/localSparkDockerPostgreSQL/readme.md).

- There are a lot of data transformations we can use while doing structured streaming. In [StatelessOperations](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/StatelessOperations.scala) we will see how some of the data transformations process each input record individually without needing any information from previous rows. So the are called **Stateless!** Also, the application has a use case of `MemoryStream` to generate data on fly and make a streaming Dataset with it, check out `addDataPeriodicallyToMemoryStream(memoryStream, interval)` method! We will also try to understand `.outputMode("")` and how it works in stateless aggregations. For more information check out [16th and 17th items in Extras](https://github.com/kantarcise/learningspark?tab=readme-ov-file#extras).

- Stateful Operations are divided into two groups (Managed and Unmanaged - page 237). Managed Stateful Operations are divided into three -> Streaming aggregations, Stream–stream joins, Streaming deduplication. We will start with Streaming aggregations:

    - Streaming Aggretations:

    - ***First***, we try to understand managed stateful operations that does not use **Time** in [ManagedStatefulAggregationsWithoutTime](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/ManagedStatefulAggregationsWithoutTime.scala) which uses a `MemoryStream` to generate a stream and shows us a lot of different types of aggregations in Structured Streaming. `allTypesOfAggregations(df: Dataframe)` is a great place to discover all kinds of aggregations. 

    - ***Second***, we will see stateful operations which **do** depend on time, with a tumbling and sliding window in [ManagedStatefulAggregationsWithTime](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/ManagedStatefulAggregationsWithTime.scala). Again, we are using `MemoryStream` to simulate a real stream, and we are working with case classes [SimpleSensorWithTime](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/SimpleSensorWithTime.scala) and [SimpleSensorWithTimestamp](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/SimpleSensorWithTimestamp.scala). We do a simple timestamp conversion too; with `map()` we convert a type of Dataset into a different Dataset! 

    - ***Third*** we will discover how we can prevent unbounded state, with [Watermarks](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/Watermarks.scala)! We will see how using a watermark effects the streaming Dataframe, with `numRowsDroppedByWatermark`.  We also have an example, [WatermarksWithSocket](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WatermarksWithSocket.scala) where our incoming data is from a socket - we use `.format("socket")` to make a streaming Dataframe! You can also play around with that!

### Use as Template 💭

If you simply want to use this repository as a template, here is the fastest way to do so.

- Install everything needed to develop Spark Applications. 

- Follow the New Project setup.  

- Depending on your data, problem and deployment, write away!

## Extras

1) Is there a Function Documentation that we can use?: Solution - [Sure, there it is.](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html)

2) How can I understand Spark's capability on Column processing ? - Solution: Here is [the link for documentation](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html), This is where the magic happens.

3) Is using [`expr()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.expr.html) bad? - Solution - If Dataframe API has what you are looking for, I think you should use that instead. Here is [an opinion.](https://stackoverflow.com/a/73645401)

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


## Offer

Maybe you've never coded in Java/Scala. You tried your luck with PySpark but it never went to being more than a small side project.

Well, with this resource, you will have a window for a [20 seconds of insane courage](https://www.youtube.com/watch?v=Ndp-_cWdxYU).

After you covered all the material, you'll be confident in your ability to solve problems with this tool.

And will be able to dive deep if needed.