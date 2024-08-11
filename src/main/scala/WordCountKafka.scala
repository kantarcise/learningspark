package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming._

/**
 * To be able to use this application, we need a
 * Kafka Cluster running
 *
 * For that we are using docker-compose
 * check out the `readme.md` inside /docker!
 */
object WordCountKafka {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .appName("StructuredKafkaWordCount")
      // intentional.
      // This one works when we compile it into a jar
      // and use spark-submit instead of running
      // it in IDE
      // .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Read from Kafka
    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "input_topic")
      .load()
      .selectExpr("CAST(value AS STRING)")

    // Split the lines into words
    val words: Dataset[String] = lines
      .as[String]
      .flatMap(_.split(" "))

    // Perform the word count using Dataset API
    val wordCounts: Dataset[WordCount] = words
      .groupByKey(identity)
      .count()
      .map { case (word, count) => WordCount(word, count) }

    // Write the word counts to Kafka
    val query: StreamingQuery = wordCounts
      .selectExpr("CAST(value AS STRING) AS key",
        "CAST(concat(value, ':', count) AS STRING) AS value")
      .writeStream
      .outputMode("complete")
      // .outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "output_topic")
      .option("checkpointLocation", "/tmp/spark_checkpoint_1")
      .start()

    query.awaitTermination()
  }
}
