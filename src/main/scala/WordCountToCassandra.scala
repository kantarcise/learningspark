package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming._

/** Just the the WordCount which counts words
 * from a socket, this application uses the same source
 * but the sink will be a Cassandra Instance!
 *
 * This is a great demonstration for foreachBatch() function.
 */
object WordCountToCassandra {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .appName("WordCountToCassandra")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Cassandra configuration
    val hostAddr = "127.0.0.1"

    // our cassandra is running on hostAddr
    spark.conf.set("spark.cassandra.connection.host", hostAddr)

    // Make a dataset representing the stream
    // of input lines from connection to localhost:9999
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

    // Write the word counts to Cassandra using foreachBatch
    val query = wordCounts
      .writeStream
      // we are using the defined method for foreachBatch
      .foreachBatch(writeCountsToCassandra _)
      .outputMode("update")
      .option("checkpointLocation", "/tmp/spark_checkpoint_2")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }

  /**
   * The method which will be used in foreachBatch, for
   * writing the results to
   * Cassandra Service.
   * @param wordCountsDS: the Dataset of WordCount
   * @param batchId: Current Batch ID
   */
  def writeCountsToCassandra(wordCountsDS: Dataset[WordCount],
                             batchId: Long): Unit = {
    val keyspaceName = "spark_keyspace"
    val tableName = "wordcount"

    wordCountsDS
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> keyspaceName))
      .mode("append")
      .save()
  }
}
