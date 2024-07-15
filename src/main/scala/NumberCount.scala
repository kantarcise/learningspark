package learningSpark

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

// Define the case class for the number counts
case class NumberCount(value: Int, count: Long)

// Only difference from WordCount is that we are
// counting Numbers now!

// We can make this application fail, if we are not careful.
// Check line 56 for that!

// To run this example, open a terminal and type `nc -lk 9999`
// After that, you can run this code in IDE.
// start typing numbers in terminal to see the number count!
object NumberCount {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .appName("StructuredNetworkNumberCount")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Make a Dataset representing the stream
    // of input lines from connection to localhost:9999
    val lines: Dataset[String] = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      .as[String]

    /*
      TIP - You cannot use direct aggregation operations like
      DataFrame.count() and Dataset.reduce() on streaming DataFrames.
      This is because, for static DataFrames, these operations immediately return
      the final computed aggregates, whereas for streaming DataFrames the
      aggregates have to be continuously updated.

      Therefore, you have to always use
      DataFrame.groupBy() or Dataset.groupByKey() for aggregations
      on streaming DataFrames.
     */

    // Split the lines into numbers
    val numbers: Dataset[Int] = lines
      .flatMap(_.split(" "))
      // .flatMap(numStr => Some(numStr.toInt))
      // if we run it like that, when we enter a string
      // in the terminal the application will crash
      // so instead, let's implement a safety net!
      .flatMap(numStr => try {
        Some(numStr.toInt)
      } catch {
        case _: NumberFormatException => None
      })

    // Perform the number count using Dataset API
    val numberCounts: Dataset[NumberCount] = numbers
      // Group by the number itself
      .groupByKey(identity)
      // Count occurrences
      .count()
      // Map to NumberCount case class
      .map { case (num, count) => NumberCount(num, count) }

    // let's define a checkpoint
    val checkpointDir = ""
    // Start running the query that
    // prints the running counts to the console
    val query: StreamingQuery = numberCounts
      .writeStream
      .outputMode("complete")
      // there are different options to discover
      //.trigger(Trigger.ProcessingTime("1 second"))
      //.trigger(Trigger.AvailableNow())
      //.trigger(Trigger.Continuous(500))
      //.option("checkpointLocation", checkpointDir)
      .format("console")
      .start()

    query.awaitTermination()

  }
}