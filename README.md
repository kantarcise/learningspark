# learningspark

All the exercises from the book "[Learning Spark](https://pages.databricks.com/rs/094-YMS-629/images/LearningSpark2.0.pdf)", solved with Dataframes and Datasets, no stone unturned.

## Why ?

There are a lot of sources on Spark, like [LearningApacheSpark](https://github.com/runawayhorse001/LearningApacheSpark), [learning-spark](https://github.com/databricks/learning-spark) from Databricks and the [updated version](https://github.com/databricks/LearningSparkV2/tree/master). Sadly, they are old and not recently updated. 

You have to wander a lot just to get the ball rolling, especially if you are new to the environment.

I thought, it shouldn't be this hard.

## Usage

You have 3 choices:

### Instuction Route - Instructions

- You can follow the instructions to make yourself a Spark playground.

### Cherry Pick Route - Code Catalog

- You can only look for what you are interested in.

### Template Route - Use as Template

- You can simply use all of the code in your projects as a template.

Choose as you wish. 

Here are all the details for routes.

### Instructions
 
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
To open the IDE.

3) To setup a project from scratch, select `New Project`, select JDK, Scala and SBT versions that we installed, make the package prefix something that you choose.

4) Now you are ready to try out some code!

### Code Catalog

Here is all the code explained in detail.

#### Chapter 0

- If you want to just see what are Dataset's and Dataframe's in Spark, you can check out [Aggregates.scala](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/Aggregates.scala)

#### Chapter 1

- This chapter is just a reading assignment. I think it is important to learn the history/roots of tools and also have a high level picture what they might look like, as we try to master them.

#### Chapter 2

- We will see our first example where we load some data onto Spark and tried to answer some questions based on the data! Processed sugar is terrible and you know it, however, here is an [example on MnM's](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/MnmCandies.scala) We will discover about relative paths, user inputs and Dataframe Schemas.

- Also here is the same example, using [only Datasets!](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/MnmCandiesDataset.scala) 

## Use as Template

If you simply want to use this repository as a template, here is the fastest way to do so.

- Install everything needed to develop Spark Applications. 

- Follow the New Project setup.  

- Depending on your data, problem and deployment, write away!

## Extras

- Is there a Function Documentation that we can use?: Solution - [Sure, there it is.](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html)

- How can I understand Spark's capability on Column processing ? - Solution: Here is [the link for documentation](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html), This is where the magic happens.

- Is using [`expr()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.expr.html) bad? - Solution - If Dataframe API has what you are looking for, I think you should use that instead. Here is [an opinion.](https://stackoverflow.com/a/73645401)

- Is there a better way to mock data? - Solution: Sure, there are [better alternatives](https://www.mockaroo.com/) than manually writing data.

- Can we clean up the code written in Dataframe API, using the Dataset API? - Solution: Yes! Depending on your definition of cleaning up, Dataset API can be pretty neat. Take a look at [EXAMPLE HERE]

- Is there a simple source that explains Repartition & Coalesce in Spark ? - Solution: This is an [attempt.](https://medium.com/@amitjoshi7/repartition-coalesce-in-apache-spark-76eb6203c316#:~:text=Use%20Coalesce%20when%20decreasing%20the%20number%20of%20partitions%20to%20reduce%20computational%20overhead.)

- How about Data Partitioning? - Solution: Here is another [link.](https://medium.com/@dipayandev/everything-you-need-to-understand-data-partitioning-in-spark-487d4be63b9c)

- How to submit applications ? - Solution - Official [docs are helpful.](https://spark.apache.org/docs/latest/submitting-applications.html)

- How to submit a Spark job, written in Scala - Solution: Although a different deployment, this is a [useful link.](https://guide.ncloud-docs.com/docs/en/hadoop-vpc-16)

- Can I get a Docker compose example - Solution  - Here [is one](https://github.com/bitnami/containers/blob/main/bitnami/spark/docker-compose.yml)

- Deploy mode - Client vs Cluster ? - Solution - An [explaination](https://stackoverflow.com/a/28808269)

- Can I get an overview about cluster mode ? - Solution - Yes for sure, [here](https://spark.apache.org/docs/latest/cluster-overview.html)

- How does `--master` selection work when submitting or writing Spark applications, is there a hierachy? - Solution - Yes there is. Apllication overrides console, console overrides `conf` - [Link from Stack Overflow](https://stackoverflow.com/a/54599477)

- What should I do If I wanted to use `sbt assembly` ? - Solution : Just use plugins.sbt to setup for assembly - [PROJECT STRUCTURE](https://stackoverflow.com/a/36186315) - add assembly to plugins.

- What if there is a merging error when I try to run `sbt clean assembly`? - Solution: Here is the assembly [merge strategy](https://stackoverflow.com/a/39058507)

## Offer

Maybe you've never coded in Java/Scala. You tried your luck with PySpark but it never went to being more than a small side project.

Well, with this resource, you will have a window for a [20 seconds of insane courage](https://www.youtube.com/watch?v=Ndp-_cWdxYU).

After you covered all the material, you'll be confident in your ability to solve problems with this tool.

And will be able to dive deep if needed.