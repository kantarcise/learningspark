package learningSpark

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.util.{Failure, Success, Try}

// Define the case class for the number counts
case class NumberCount(value: Int, count: Long)

/**
 * Structured Streaming application that counts numbers
 * from a TCP socket.
 *
 * To run this example, open a terminal and type `nc -lk 9999`
 * (which will make a simple TCP server).
 *
 * After that, you can run this code in your IDE.
 * Start typing numbers in the terminal to see the number count!
 */
object NumberCount {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder
      .appName("Structured Network Number Count")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Make a Dataset representing the stream of
    // input lines from connection to localhost:9999
    val lines: Dataset[String] = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      .as[String]

    // Split the lines into numbers, implementing
    // a safety net to avoid NumberFormatException
    val numbers: Dataset[Int] = lines
      .flatMap(_.split(" "))
      .flatMap(numStr => Try(numStr.toInt) match {
        case Success(num) => Some(num)
        case Failure(_) => None
      })

    // Perform the number count using Dataset API
    val numberCounts: Dataset[NumberCount] = numbers
      .groupByKey(identity)
      .count()
      .map { case (num, count) => NumberCount(num, count) }

    // Start running the query that prints the running
    // counts to the console
    val query: StreamingQuery = numberCounts
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    // Start a separate thread to print
    // the last progress periodically
    startProgressReporter(query)
    query.awaitTermination()
  }

  /**
   * Starts a thread that periodically prints the last progress of the streaming query.
   *
   * @param query the streaming query
   */
  private def startProgressReporter(query: StreamingQuery): Unit = {
    val progressThread = new Thread(new Runnable {
      def run(): Unit = {
        while (true) {
          val progress = query.lastProgress
          if (progress != null) {
            println(progress)
          }
          Thread.sleep(10000) // Sleep for 10 seconds
        }
      }
    })

    progressThread.setDaemon(true)
    progressThread.start()
  }
}
