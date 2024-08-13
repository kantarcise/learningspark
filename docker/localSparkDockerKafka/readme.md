# Spark Streaming with Kafka! ⛵

## What is going on ?

To be able to use [this application](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountKafka.scala), we need a Kafka Cluster running. Wait, what is that mean? Here is [a wonderful explanation.](https://www.boltic.io/blog/kafka-clusters#:~:text=To%20get%20started%2C%20A%20Kafka,speed%2C%20fault%2Dtolerant%20network.)

To deploy Kafka, we are using docker and [docker compose](https://docs.docker.com/compose/). A simple introduction [can be found here.](https://stackoverflow.com/a/78745267)

To install docker engine, you can follow [these installation steps.](https://docs.docker.com/engine/install/ubuntu/)

## After Docker & Docker Compose Installation

### Fire up Containers

Let's fire up your containers first.

In this directory (where the readme.md you reading is), open a terminal and run:

```bash
docker compose up
```

Depending on whether you have the images in your local system, this may take some time. No worries!

In a new terminal, run:

```bash
watch -n 1 docker ps -a
```

To monitor that your containers are up and running!

### Run Kafka scripts inside Containers

In the WordCount application we used `nc -lk 9999` in terminal. 

Now, we get to use some Kafka command line tools or Kafka Shell Scripts!

Let's type some words to our producer topic and see the results in our consumer topic.

Open 2 terminals and get inside the kafka container (one for producer one for consumer).

```bash
docker exec -it kafkaContainer /bin/bash
```

In one of them run the **producer**:

```bash
kafka-console-producer.sh --broker-list kafka:9093 --topic input_topic
```

The other will be **consumer**, which will show us the results coming from Spark! You can remove the `--from-beginning` if you want.

```bash
kafka-console-consumer.sh --bootstrap-server kafka:9093 --topic output_topic --from-beginning
```

### Deployment - Compile the Scala code and package it into a `.jar` file.

The only step left is to run our Spark Application! Let's now get the jar first, for `WordCountKafka`.

In Intellij IDE, you can use SBT shell. Here is an [example screenshot](https://tarjotin.cs.aalto.fi/CS-A1140/2022/instructions/_images/intellij-sbt-console.png) about where it is! 

Run: 

```sbt
clean
```

And

```sbt
package
```

after.

Now you have a jar file in your repository, under `/YOUR_PATH_FOR_REPOSITORY/learningspark/target/scala-2.12/`!

Get to you local spark directory, and run the example.

```bash
cd /where/you/extracted/spark/binaries

# example
cd /usr/local/spark
```

We will use, spark-submit to launch the application. 

Submit your jar, with `spark-sql-kafka-0-10_2.12:3.5.0` package!

Example (change your jar name accordingly):

```bash
./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --class learningSpark.WordCountKafka --master local[8] /home/sezai/IdeaProjects/learningspark/target/scala-2.12/learningspark_2.12-0.1.0-SNAPSHOT.jar
```

Now you can type some words inside the terminal that has KafkaProducer running.

You will see the results in the consumer terminal! 


## Extras


If you want to understand what we did inside [docker-compose.yml](https://github.com/kantarcise/learningspark/blob/main/docker/docker-compose.yml) you can check [out this wonderful article](https://rmoff.net/2018/08/02/kafka-listeners-explained/).

For spark jar, you can use the assembly route too! - Assembly [explained.](https://spark.apache.org/docs/latest/submitting-applications.html#bundling-your-applications-dependencies)

Really good resource for [Bundling dependencies](https://spark.apache.org/docs/latest/submitting-applications.html#bundling-your-applications-dependencies).

SparkStreaming - Kafka - [Deploying](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#deploying).