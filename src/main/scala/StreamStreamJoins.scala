package learningSpark

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration

/**
 * Now that we have 2 streaming Dataframes, we will
 * have 2 MemoryStreams with 2 different methods.
 */
object StreamStreamJoins {
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession
      .builder
      .appName("Stream-Stream Joins Dataframe")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Initialize MemoryStreams
    val clickMemoryStream = new MemoryStream[Click](1, spark.sqlContext)
    val impressionMemoryStream = new MemoryStream[Impression](1, spark.sqlContext)

    // Add sample data every second - one by one
    val addDataFutureOne = addDataPeriodicallyToClickMemoryStream(
      clickMemoryStream,
      1.seconds)

    // every second
    val addDataFutureTwo = addDataPeriodicallyToImpressionMemoryStream(
      impressionMemoryStream,
      1.seconds)

    val clickStream: DataFrame = clickMemoryStream
      .toDF()

    val impressionStream: DataFrame = impressionMemoryStream
      .toDF()

    // This was the regular inner join
    // 3 instances where joining happens
    // val joinedStream = impressionStream
    //  .join(clickStream, "adId")

    // Because this joining is expensive and
    // Structured Streaming will buffer all non matched
    // Clicks and Impressions, in it's the state,
    // let's use watermarking!
    val joinedStream = joinTwoStreamingDataframesWithWatermark(
      // impressionStream, clickStream, "inner")
      // We may want all ad impressions to be reported,
      // with or without the associated click data,
      // to enable additional analysis later
      // just select outer join!
      impressionStream, clickStream, "leftouter")

    val query = joinedStream
      .writeStream
      .queryName("Joined stream to Console")
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()

    query.awaitTermination()

    // Wait for the data adding to finish (it won't, but in a real
    // use case you might want to manage this better)
    Await.result(addDataFutureOne, Duration.Inf)
    Await.result(addDataFutureTwo, Duration.Inf)
  }

  /**
   * Add generated data on time interval, to an existing MemoryStream.
   * This simulates a real streaming scenario where data arrives continuously.
   *
   * We are making a Impression stream! Based on the data we have,
   * there will be 3 occasions that is joined.
   */
  def addDataPeriodicallyToImpressionMemoryStream(memoryStream: MemoryStream[Impression],
                                             interval: FiniteDuration): Future[Unit] = Future {

    // 3 matching ID impressions, in different times
    val sampleData: Seq[Impression] = Seq(
      Impression("eebc1f79-03df-4b17-8124-f4875a0e1f65", Timestamp.valueOf("2024-07-09 12:31:56.789"), "user_0", true, "mobile"),
      // these 3 Impressions will have no chance to be joined, and because of
      // that they will be in the output when leftouter joining is used!
      Impression("8a7c9a5a-6d1b-48b1-9a92-1f2a334b8a2c", Timestamp.valueOf("2024-07-09 12:34:55.123"), "user_1", false, "desktop"),
      Impression("d7a1b2c3-4e5f-6a7b-8c8d-9f0e7e6a2c4d", Timestamp.valueOf("2024-07-09 12:42:52.555"), "user_4", true, "desktop"),
      Impression("a8b9c0d1-2e3f-4a5b-6c7d-8e9f0a1b2c3d", Timestamp.valueOf("2024-07-09 12:45:49.371"), "user_7", false, "desktop"),
      // the matched one
      Impression("c1d2e3f4-5a6b-7c8d-9e0f-1a2b3c4d5e6f", Timestamp.valueOf("2024-07-09 12:45:57.907"), "user_8", true, "tablet"),
      Impression("g5g5e3f4-432d-asd2-htf3-1as32f325e77", Timestamp.valueOf("2024-07-09 13:48:57.888"), "user_9", true, "tablet"),
    )

    sampleData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }
  }

  /**
   * Add generated data on time interval, to an existing MemoryStream.
   * This simulates a real streaming scenario where data arrives continuously.
   *
   * We are making a Click stream! Based on the data we have,
   * there will be 3 occasions that is joined.
   */
  def addDataPeriodicallyToClickMemoryStream(memoryStream: MemoryStream[Click],
                                        interval: FiniteDuration): Future[Unit] = Future {

    // let's just have 3 matching ID, clicks, one of them will be late
    // in inner join
    val sampleData: Seq[Click] = Seq(
      Click("eebc1f79-03df-4b17-8124-f4875a0e1f65", Timestamp.valueOf("2024-07-09 12:35:56.789"), "USA"),
      Click("38x6zbhc-6e7b-4e1b-9a1e-2f7a334c5a2b", Timestamp.valueOf("2024-07-09 12:38:53.234"), "BLR"),
      Click("d7a1b2c3-4e5f-6a7b-8c8d-9f0e7e6a2c4d", Timestamp.valueOf("2024-07-09 12:40:52.341"), "BRA"),
      Click("c1d2e3f4-5a6b-7c8d-9e0f-1a2b3c4d5e6f", Timestamp.valueOf("2024-07-09 12:53:48.973"), "USA"),
      Click("zjleics9-1c2d-3e4f-5a6b-7c8d9e0f1a2b", Timestamp.valueOf("2024-07-09 12:59:47.789"), "TUN"),
      Click("6561z26w-8c9d-0e1a-2b3c-4d5e6f7a8b9c", Timestamp.valueOf("2024-07-09 13:44:50.444"), "BRA"),
    )
    sampleData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }

  }

  /** When we join two streaming Dataframes, the engine will
   * buffer all clicks and impressions as state, and will generate
   * a matching impression-and-click as soon as a received click
   * matches a buffered impression (or vice versa, depending
   * on which was received first).
   *
   * To limit the streaming state maintained by stream–stream
   * joins, we need to know the following
   * information about your use case:
   *
   * - • What is the maximum time range between the
   * generation of the two events at their respective sources?
   *
   * - • What is the maximum duration an event can be
   * delayed in transit between the source and the processing engine?
   *
   * For details, see page 249-250 on the book.
   *
   * In our clickStream Dataframe, one of the Clicks
   * is not in the interval, and it will be dropped.
   *
   * Result will have 2 instances joined!
   *
   * @param impressionsDF: Dataframe Left
   * @param clicksDF: Dataframe Right
   * @param typeOfJoin: String  Type of Joining wanted
   * @return impressionsWithWatermark: Joined Dataframe
   */
  def joinTwoStreamingDataframesWithWatermark(impressionsDF: DataFrame,
                                              clicksDF: DataFrame,
                                              typeOfJoin: String): DataFrame = {

    val impressionsWithWatermark = impressionsDF
      .selectExpr("adId AS impressionAdId", "impressionTime")
      .withWatermark("impressionTime", "10 minutes")

    val clicksWithWatermark = clicksDF
      .selectExpr("adId AS clickAdId", "clickTime")
      .withWatermark("clickTime", "10 minutes")

    // Inner join with time range conditions
    impressionsWithWatermark
      .join(clicksWithWatermark,
      expr(
        """clickAdId = impressionAdId AND
          |clickTime BETWEEN impressionTime AND impressionTime + interval 15 minutes""".stripMargin)
      , typeOfJoin)
  }
}
