package learningSpark

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import java.sql.Timestamp

/**
 * Enhanced tests for Stream-Stream Left Outer Joins with Watermarks
 */
class StreamStreamJoinsTest extends AnyFunSuite with Eventually with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("Stream-Stream Joins Dataframe Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Stream-Stream Left Outer Join with Watermark - Basic Scenario") {

    // Initialize MemoryStreams
    val clickMemoryStream = new MemoryStream[Click](1, spark.sqlContext)
    val impressionMemoryStream = new MemoryStream[Impression](1, spark.sqlContext)

    // Start adding data asynchronously
    val addDataFutureOne = StreamStreamJoins
      .addDataPeriodicallyToClickMemoryStream(clickMemoryStream, 1.second)

    val addDataFutureTwo = StreamStreamJoins
      .addDataPeriodicallyToImpressionMemoryStream(impressionMemoryStream, 1.second)

    val clickStream: DataFrame = clickMemoryStream.toDF()
    val impressionStream: DataFrame = impressionMemoryStream.toDF()

    // Perform the join operation with left outer join
    val joinedStream = StreamStreamJoins.joinTwoStreamingDataframesWithWatermark(
      impressionStream, clickStream, "leftouter"
    )

    // Start the streaming query and collect results to memory
    val query = joinedStream
      .writeStream
      .outputMode("append")
      .format("memory")
      .queryName("joinedStreamBasic")
      .start()

    try {
      // Wait for the data adding to finish
      Await.result(addDataFutureOne, Duration.Inf)
      Await.result(addDataFutureTwo, Duration.Inf)

      // Allow some time for the streaming query to process the data
      eventually(timeout(30.seconds), interval(2.seconds)) {

        // we can directly query using a query name!
        val result = spark.sql("SELECT * FROM joinedStreamBasic")

        // Debug: Print schema and data
        println("Schema of joinedStreamBasic:")
        result.printSchema()
        println("Data in joinedStreamBasic:")
        result.show(truncate = false)

        // Expected: 5 impressions, 2 matched with clicks, 3 with NULLs
        assert(result.count() == 5)

        // Define expected matched records
        val expectedMatches = Seq(
          ("eebc1f79-03df-4b17-8124-f4875a0e1f65",
            Timestamp.valueOf("2024-07-09 12:31:56.789"),
            Timestamp.valueOf("2024-07-09 12:35:56.789")),
          ("c1d2e3f4-5a6b-7c8d-9e0f-1a2b3c4d5e6f",
            Timestamp.valueOf("2024-07-09 12:45:57.907"),
            Timestamp.valueOf("2024-07-09 12:53:48.973"))
        ).map { case (adId, impressionTime, clickTime) =>
          (adId, impressionTime, Some(clickTime))
        }

        // Define expected unmatched records
        val expectedUnmatched = Seq(
          ("d7a1b2c3-4e5f-6a7b-8c8d-9f0e7e6a2c4d",
            Timestamp.valueOf("2024-07-09 12:42:52.555")),
          ("8a7c9a5a-6d1b-48b1-9a92-1f2a334b8a2c",
            Timestamp.valueOf("2024-07-09 12:34:55.123")),
          ("a8b9c0d1-2e3f-4a5b-6c7d-8e9f0a1b2c3d",
            Timestamp.valueOf("2024-07-09 12:45:49.371"))
        ).map { case (adId, impressionTime) =>
          (adId, impressionTime, None)
        }

        // Combine expected data
        val expectedData = expectedMatches ++ expectedUnmatched

        // Iterate over each expected record in the
        // combined expected data (matched and unmatched)
        expectedData.foreach { case (adId, impTime, clickTimeOpt) =>

          // Make a filter expression to locate the
          // specific records
          // We filter where 'impressionAdId' matches
          // 'adId' and 'impressionTime' matches 'impTime'
          val filterExpr = F.col("impressionAdId") === adId &&
            F.col("impressionTime") === impTime

          // Apply the filter to the result DataFrame to extract the specific record
          val df = result.filter(filterExpr)

          // Assert that exactly one record matches the filter criteria
          // This ensures there are no duplicate or
          // missing records for the given 'adId' and 'impressionTime'
          assert(df.count() == 1, s"Expected one record for adId: $adId")

          // Retrieve the first (and only) row from the filtered DataFrame
          // Since we asserted that count is 1, it's safe to call 'first()' here
          val row = df.first()

          // Assert that the 'impressionAdId' in the row matches the expected 'adId'
          assert(row.getAs[String]("impressionAdId") == adId,
            s"ImpressionAdId mismatch for adId: $adId")

          // Assert that the 'impressionTime' in the row matches the expected 'impTime'
          assert(row.getAs[Timestamp]("impressionTime") == impTime,
            s"ImpressionTime mismatch for adId: $adId")

          // Handle assertions based on
          // whether a click is expected for this impression
          clickTimeOpt match {
            case Some(ct) =>
              // **Case 1: A matching click is expected**

              // Assert that 'clickTime' in the row matches the expected 'ct' (Click Time)
              assert(row.getAs[Timestamp]("clickTime") == ct,
                s"Click time mismatch for adId: $adId")

              // Assert that 'clickAdId' in the row matches the expected 'adId'
              // This ensures the click is correctly associated with the impression
              assert(row.getAs[String]("clickAdId") === adId,
                s"ClickAdId mismatch for adId: $adId")

            case None =>
              // **Case 2: No matching click is expected**

              // Assert that 'clickTime' is NULL in the row since there is no matching click
              assert(row.isNullAt(row.fieldIndex("clickTime")),
                s"Expected NULL clickTime for adId: $adId")

              // Assert that 'clickAdId' is NULL in the row since there is no matching click
              assert(row.isNullAt(row.fieldIndex("clickAdId")),
                s"Expected NULL clickAdId for adId: $adId")
          }
        }
      }
    }
  }
}
