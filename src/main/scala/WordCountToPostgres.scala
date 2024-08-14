package learningSpark

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.streaming.Trigger

/**
 * How about using a PosgtreSQL Sink?
 *
 * Check out the readme in localSparkDockerPostgreSQL
 * to start a PosgtreSQL instance with docker.
 */
object WordCountToPostgres {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder
      .appName("WordCountToPostgres")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // PostgreSQL configuration
    val url = "jdbc:postgresql://localhost:5432/sparkdb"
    val user = "spark"
    val password = "spark"

    // Instantiate the CustomForeachWriter
    val customForeachWriter = new CustomForeachWriter(url, user, password)

    // Make a dataset representing the stream of input
    // lines from connection to localhost:9999
    val linesDS: Dataset[String] = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      .as[String]

    // Split the lines into words
    val words: Dataset[String] = linesDS
      .flatMap(_.split(" "))

    // Perform the word count using Dataset API
    val wordCounts: Dataset[WordCount] = words
      .groupByKey(identity)
      .count()
      .map { case (word, count) => WordCount(word, count) }

    // Write the word counts to PostgreSQL using foreach
    val query = wordCounts
      .writeStream
      .queryName("Word Count to PostgreSQL")
      .foreach(customForeachWriter)
      .outputMode("update")
      .option("checkpointLocation", "/tmp/spark_checkpoint_3")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
