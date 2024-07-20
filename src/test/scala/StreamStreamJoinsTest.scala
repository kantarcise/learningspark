package learningSpark
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * Let's test out approach where use use Watermarks and
 * Event-time Interval for an Outer Join!
 */
class StreamStreamJoinsTest extends AnyFunSuite with Eventually {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("Stream-Stream Joins Dataframe Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  test("Stream-Stream Left Outer Join with Watermark") {

    // Initialize MemoryStreams
    val clickMemoryStream = new MemoryStream[Click](1, spark.sqlContext)
    val impressionMemoryStream = new MemoryStream[Impression](1, spark.sqlContext)

    // Add sample data every second - one by one
    val addDataFutureOne = StreamStreamJoins.addDataPeriodicallyToClickMemoryStream(
      clickMemoryStream, 1.seconds)

    val addDataFutureTwo = StreamStreamJoins.addDataPeriodicallyToImpressionMemoryStream(
      impressionMemoryStream, 1.seconds)

    val clickStream: DataFrame = clickMemoryStream.toDF()
    val impressionStream: DataFrame = impressionMemoryStream.toDF()

    // Perform the join operation with left outer join
    val joinedStream = StreamStreamJoins.joinTwoStreamingDataframesWithWatermark(
      impressionStream, clickStream, "leftouter")

    // Start the streaming query and collect results to memory
    val query = joinedStream
      .writeStream
      .outputMode("append")
      .format("memory")
      .queryName("joinedStream")
      .start()

    try {
      // Wait for the data adding to finish
      Await.result(addDataFutureOne, Duration.Inf)
      Await.result(addDataFutureTwo, Duration.Inf)

      // When we use left outer, we will see 5 Impressions!
      // 2 joined with Click, 3 with NULLs
      eventually(timeout(30.seconds), interval(2.seconds)) {
        val result = spark.sql("SELECT * FROM joinedStream")
        // Check the counts based on the expected results
        assert(result.count() == 5) // Adjust based on your expectations
        result.show(truncate = false)
      }
    } finally {
      query.stop()
    }
  }
}