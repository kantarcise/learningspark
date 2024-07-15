# Spark Streaming to PostrgeSQL 🪟

## What is going on ?

For [this application](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountToPostgres.scala), we need a PostgreSQL Instance up an running to test our approach.

Let's use docker-compose.

### 1) Fire up Container ✨

Let's fire up your container first.

In this directory (where the readme.md you reading is), open a terminal and run:

```bash
docker compose up
```

Depending on whether or not you have the images in your local system, this may take some time. No worries!

In a new terminal, run:

```bash
watch -n 1 docker ps -a
```

To monitor that your PostgreSQL container are up and running!

### 2) Make Keyspace and Table in PostgreSQL: 📜

In a terminal 

```bash
docker exec -it postgres psql -U spark -d sparkdb
```

In the `psql` prompt, execute the following commands, to make sure the tables are made:

```sql
CREATE TABLE IF NOT EXISTS wordcount (
  value TEXT PRIMARY KEY,
  count BIGINT
);
```

### 3) Start a Socket Text Stream ✏️

Just like we did in [WordCount](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCount.scala), open a simple socket server to type in words!

```bash
nc -lk 9999
```

### 4) Run the Spark Application

We are using the [PostgreSQL JDBC Driver](https://mvnrepository.com/artifact/org.postgresql/postgresql/42.4.5) and we can configure it as a `--package` while we are submitting our application.

Just like in [WordCountKafka](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountKafka.scala) get a jar with `sbt clean package` and submit the application with:

```bash
./bin/spark-submit --packages org.postgresql:postgresql:42.4.5 --class learningSpark.WordCountToPostgres --master local[*] /home/sezai/IdeaProjects/learningspark/target/scala-2.12/learningspark_2.12-0.1.0-SNAPSHOT.jar
```

Change the jar name/path according to your filesystem.

If this is not clear, refer to [readme from WordCountKafka](https://github.com/kantarcise/learningspark/blob/main/docker/localSparkDockerKafka/readme.md).

### 5) Let's see the results! 🎈

Get inside container:

```bash
docker exec -it postgres psql -U spark -d sparkdb
```

And inside psql, run:

```sql
SELECT * FROM wordcount;
```

Depending on the words you typed, you should see something like:

```bash
 value | count 
-------+-------
 hello |     2
 spark |     1
 world |     1
```