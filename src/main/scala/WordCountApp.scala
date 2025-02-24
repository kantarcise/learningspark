package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming._

// Define the case class for the word counts
case class WordCount(value: String, count: Long)

/**
 * Welcome to the World of Streaming with Spark!
 *
 * To run this example, open a terminal and type `nc -lk 9999`
 *
 * After that, you can run this code in IDE.
 *
 * Simply, start typing words in terminal to see
 * the word count!
 */
object WordCountApp {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .appName("Structured Network WordCount")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Make a Dataset representing the stream
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
      // split by whitespace
      .flatMap(word => word.split(" "))

    // Dataframe API
    // Generate running word count
    // val wordCounts = words
    //   .groupBy("value")
    //   .count()

    // Perform the word count using Dataset API
    val wordCounts: Dataset[WordCount] = words
      // Group by the word itself
      .groupByKey(identity)
      // Count occurrences
      .count()
      // Map to WordCount case class
      // we have a string and long at the moment,
      // we want to map them!
      .map { case (word, count) => WordCount(word, count) }

    // We can add a custom listener to Spark!
    // check out CustomListener - book page 225
    // val myListener = new CustomListener
    // spark.streams.addListener(myListener)

    // Start running the query that
    // prints the running counts to the console
    val query: StreamingQuery = wordCounts
      .writeStream
      // we are doing a stateful operation
      // meaning the count can / will update a previously
      // generated result
      // so append mode will not work.
      // if you try you will get the error:
      // Append output mode not supported when there are streaming
      // aggregations on streaming DataFrames/DataSets
      // without watermark
      .outputMode("complete")
      .format("console")
      .start()

    // Aside from the information on UI, we have
    // query.lastProgress, here is a method on it
    
    // Call the method to print progress every 10 seconds
    // printProgress(query, 10000)
    query.awaitTermination()

  }

  // Method to print lastProgress periodically
  def printProgress(query: StreamingQuery, interval: Long): Unit = {
    new Thread(new Runnable {
      def run(): Unit = {
        while (true) {
          // book page 224 for detailed information.
          println(query.lastProgress)
          Thread.sleep(interval)
        }
      }
    }).start()
  }
}
