package learningSpark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/** We are using the Eventually Trait from ScalaTest
 * to repeatedly execute a block of code until it either
 * succeeds or a specified timeout is reached.
 *
 *  It is particularly useful in scenarios where the outcome
 *  is expected to become true eventually, such as in
 *  the case of asynchronous operations or when testing
 *  streaming applications.
 */
class StreamStaticJoinsDataframeTest extends AnyFunSuite with Eventually {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("Stream Static Joins Dataframe Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  test("Static and Streaming DataFrame Join") {

    val impressionsDF: DataFrame = StreamStaticJoinsDataframe
      .readStaticDataFrame(spark)

    // Initialize MemoryStream for the Clicks
    val clickMemoryStream = new MemoryStream[Click](1, spark.sqlContext)

    // Add sample data every 2 seconds - one by one
    val addDataFuture: Future[Unit] = StreamStaticJoinsDataframe
      .addDataPeriodicallyToMemoryStream(clickMemoryStream, 2.seconds)

    val clickStream: DataFrame = clickMemoryStream
      .toDF()

    // Perform the join operation
    val joinedStream = clickStream
      .join(impressionsDF, Seq("adId"))

    // Start the streaming query and collect results to memory
    val query: StreamingQuery = joinedStream
      .writeStream
      .queryName("Joined Stream within Memory")
      .outputMode("append")
      .format("memory")
      .queryName("joinedStream")
      .start()

    try {
      // Wait for the data adding to finish
      Await.result(addDataFuture, Duration.Inf)

      // Check the data with given intervals
      eventually(timeout(30.seconds), interval(2.seconds)) {
        val result = spark.sql("SELECT * FROM joinedStream")
        // 3 instances should be joined
        assert(result.count() == 3)
        result.show(truncate = false)
      }
    } finally {
      query.stop()
    }
  }
}