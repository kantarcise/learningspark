package learningSpark

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import java.sql.Timestamp
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration

/** Structured Streaming supports joining a streaming
 * Dataset with another static or streaming Dataset.
 *
 * Now, we will discover how Spark joins a static
 * Dataframe with a streaming one!
 */
object StreamStaticJoinsDataset{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Stream Static Joins Dataset")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val impressionsDS = readStaticDataset(spark)

    // let's cache the impressionsDS, it will be read repeatedly.
    impressionsDS.cache()

    // Initialize MemoryStream for the Clicks
    val clickMemoryStream = new MemoryStream[Click](1, spark.sqlContext)

    // Add sample data every 2 seconds - one by one
    val addDataFuture = addDataPeriodicallyToMemoryStream(clickMemoryStream,
      2.seconds)

    val clickStream: Dataset[Click] = clickMemoryStream
      .toDS()

    // Perform the join operation
    // Using `joinWith` - from Typed transformations
    val joinedStream = clickStream
      .joinWith(impressionsDS, clickStream("adId") === impressionsDS("adId"))

    // Start the streaming query and print to console
    val query = joinedStream
      .writeStream
      .queryName("Joined Click and Impressions to Console")
      .outputMode("append")
      .format("console")
      .option("truncate", value = false)
      .start()

    query.awaitTermination()

    // Wait for the data adding to finish (it won't, but in a real
    // use case you might want to manage this better)
    Await.result(addDataFuture, Duration.Inf)
  }

  /** Let's suppose we are an advertisement company that shows
   * ads on websites and you make money when users click on them.
   *
   * Assume that we have a static Dataset of all the ads to
   * be shown (known as impressions), and another stream of events
   * for each time users click on the displayed ads.
   *
   * To calculate the click revenue, you have to match each
   * click in the event stream to the corresponding ad
   * impression in the table.
   *
   * Let's read the static Dataset now.
   */
  def readStaticDataset(spark: SparkSession) : Dataset[Impression] = {
    import spark.implicits._

    val impressionsSchema = StructType(Seq(
      StructField("adId", StringType, nullable = true),
      StructField("impressionTime", TimestampType, nullable = true),
      StructField("userId", StringType, nullable = true),
      StructField("clicked", BooleanType, nullable = true),
      StructField("deviceType", StringType, nullable = true)
    ))

    // Define the data path as a val
    val impressionsFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/impressions.csv"
    }

    spark
      .read
      .schema(impressionsSchema)
      // instead of defining the schema we could
      // .schema(implicitly[Encoder[Impression]].schema)
      .option("header", "true")
      .format("csv")
      .load(impressionsFilePath)
      .as[Impression]
  }

  /**
   Add generated data on time interval, to an existing MemoryStream.
   This simulates a real streaming scenario where data arrives continuously.

   We are making a Click stream! Based on the data we have,
   there will be 3 occasions that is joined.
   */
  def addDataPeriodicallyToMemoryStream(memoryStream: MemoryStream[Click],
                                        interval: FiniteDuration): Future[Unit] = Future {

    val sampleData: Seq[Click] = Seq(
      Click("eebc1f79-03df-4b17-8124-f4875a0e1f65", Timestamp.valueOf("2024-07-09 12:35:00"), "USA"),
      Click("yrhh4w5j-6d1b-48b1-9a92-1f2a334b8a2c", Timestamp.valueOf("2024-07-09 12:37:00"), "USA"),
      Click("f9eayav9-4d5b-4d3c-8f8f-df2a77e7e3f1", Timestamp.valueOf("2024-07-09 12:39:00"), "ARG"),
      Click("38x6zbhc-6e7b-4e1b-9a1e-2f7a334c5a2b", Timestamp.valueOf("2024-07-09 12:41:00"), "BLR"),
      Click("d7a1b2c3-4e5f-6a7b-8c8d-9f0e7e6a2c4d", Timestamp.valueOf("2024-07-09 12:43:00"), "BRA"),
      Click("azywy8h9-6d7e-8f9a-1b0c-2d3e4f5a6b7c", Timestamp.valueOf("2024-07-09 12:45:00"), "LBN"),
      Click("6561z26w-8c9d-0e1a-2b3c-4d5e6f7a8b9c", Timestamp.valueOf("2024-07-09 12:47:00"), "BRA"),
      Click("8d0yq0vm-2e3f-4a5b-6c7d-8e9f0a1b2c3d", Timestamp.valueOf("2024-07-09 12:49:00"), "BRA"),
      Click("c1d2e3f4-5a6b-7c8d-9e0f-1a2b3c4d5e6f", Timestamp.valueOf("2024-07-09 12:51:00"), "USA"),
      Click("zjleics9-1c2d-3e4f-5a6b-7c8d9e0f1a2b", Timestamp.valueOf("2024-07-09 12:53:00"), "TUN")
    )

    sampleData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }
  }
}