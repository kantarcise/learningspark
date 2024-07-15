# Spark Streaming to Cassandra 🍎

## What is going on ?

For [this application](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountKafka.scala), we need a Cassandra Instance up an running to test our approach.

Let's use docker-compose.

### 1) Fire up Container ✨

Let's fire up your containers first.

In this directory (where the readme.md you reading is), open a terminal and run:

```bash
docker compose up
```

Depending on whether or not you have the images in your local system, this may take some time. No worries!

In a new terminal, run:

```bash
watch -n 1 docker ps -a
```

To monitor that your Cassandra container are up and running!

### 2) Make Keyspace and Table in Cassandra: 📜

In a terminal 

```bash
docker exec -it cassandra cqlsh
```

In the `cqlsh` prompt (`cqlsh` is a command-line interface for interacting with Cassandra using CQL (the Cassandra Query Language) [refer here for more information](https://cassandra.apache.org/doc/stable/cassandra/tools/cqlsh.html)), execute the following commands:

```sql
CREATE KEYSPACE spark_keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE spark_keyspace;

CREATE TABLE wordcount (
  value text PRIMARY KEY,
  count bigint
);
```

### 3) Start a Socket Text Stream ✏️

Just like we did in [WordCount](), open a simple socket server to type in words!

```bash
nc -lk 9999
```

### 4) Run the Spark Application

Just like in [WordCountKafka](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountKafka.scala) get a jar with `sbt clean package` and submit the application with:

Now that we are using a Cassandra connection, we should add the [Spark Cassandra Connector](https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector_2.12/3.5.0) Dependency for it! Here [is more](https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management) for the curious minds.

```bash
./bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 --class learningSpark.WordCountToCassandra --master local[*] path/to/your/jarfile.jar
```

If this is not clear, refer to [readme from WordCountKafka](https://github.com/kantarcise/learningspark/blob/main/docker/localSparkDockerKafka/readme.md).

### 5) Let's see the results! 🎈

Get inside container:

```bash
docker exec -it cassandra cqlsh
```

And inside the cqlsh, run:

```sql
USE spark_keyspace;
SELECT * FROM wordcount;
```

Depending on the words you typed, you should see something like:

```bash
 value   | count
---------+-------
       a |     2
 writing |     2
    this |     2
    test |     4
```