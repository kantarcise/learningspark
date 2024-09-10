package learningSpark

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.StreamingQuery
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration

import java.sql.Timestamp

/** With watermarks, we will know the point at which no more data
 * will arrive for a given group, and the engine can automatically
 * finalize the aggregates of certain groups and drop them  from the state.
 *
 * This limits the total amount of state that the engine has to
 * maintain to compute the results of the query :)
 *
 * In our application we will have watermarked Datasets
 * and a simulated data which will arrive later than our watermark
 *
 * They will be dropped as we expect!
 */
object Watermarks {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Watermarks")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // with the help of CustomListener, we can see information like
    // numRowsDroppedByWatermark
    spark.streams.addListener(new CustomListener)

    // Initialize MemoryStream
    val sensorMemoryStream = new
        MemoryStream[SimpleSensorWithTimestamp](1, spark.sqlContext)

    // Add sample data every 3 seconds - one by one!
    val addDataFuture = addDataPeriodicallyToMemoryStream(
      sensorMemoryStream, 3.seconds)

    val sensorStreamWithTimestamp: Dataset[SimpleSensorWithTimestamp] = sensorMemoryStream
      .toDS()

    // Perform aggregation, tumblingWindow
    // Our window will start at {2022-01-02 10:00:00}
    // and it will end in 2022-01-02 10:04:00.
    val aggregatedStreamTumbling: DataFrame = sensorStreamWithTimestamp
      // two-minute windows as a dynamically computed grouping column.
      .withWatermark("eventTime", "2 minutes")
      .groupBy(window($"eventTime", "2 minutes"), $"device_id")
      .count()

    // Perform aggregation, slidingWindow
    // Our window will be {2022-01-02 09:59:00, 2022-01-02 10:01:00}
    // to {2022-01-02 10:02:00, 2022-01-02 10:04:00} - 1 minute sliding
    // some events will be in multiple groups!
    val aggregatedStreamSliding: DataFrame = sensorStreamWithTimestamp
      .withWatermark("eventTime", "2 minutes")
      .groupBy(window(
        timeColumn =  $"eventTime",
        windowDuration =  "2 minutes",
        slideDuration =  "1 minute"), $"device_id")
       .count()

    // In the book, it says this about the `update` mode:
    //      - This is the most useful and efficient mode to
    // run queries with streaming aggregations.

    // This query will drop 8 rows, because every instance is
    // in a single window
    val query: StreamingQuery = aggregatedStreamTumbling
      .writeStream
      .queryName("Tumbling Window Stream")
      // TODO: In complete mode, watermarks doesn't work. Why?
      //  The answer in the book, end of page 245!
      .outputMode("update")
      .format("console")
      // we can see the print out in full
      .option("truncate" , value = false)
      .start()

    // this query will drop 16 rows, because of sliding window
    val querySecond: StreamingQuery = aggregatedStreamSliding
      .writeStream
      .queryName("Sliding Window Stream")
      .outputMode("update")
      .format("console")
      .option("truncate", value = false)
      .start()

    // in the console, we will see
    // the numRowsDroppedByWatermark :)
    query.awaitTermination()
    querySecond.awaitTermination()

    // Wait for the data adding to finish (it won't, but in a real
    // use case you might want to manage this better)
    Await.result(addDataFuture, Duration.Inf)
  }

  /**
   Add generated data on time interval, to an existing MemoryStream.

   This simulates a real streaming scenario where data arrives continuously.

   This time, let's use timestamps directly. :)
   */
  def addDataPeriodicallyToMemoryStream(memoryStream: MemoryStream[SimpleSensorWithTimestamp],
                                        interval: FiniteDuration): Future[Unit] = Future {

    // Random temperatures - in between 20 and 80 celsius
    val r = new scala.util.Random

    // every sensor in every other minute, with some late data!
    val testData: Seq[SimpleSensorWithTimestamp] = Seq(
      SimpleSensorWithTimestamp(1, 20 + r.nextInt(61), Timestamp.valueOf("2022-01-02 10:00:00")),
      SimpleSensorWithTimestamp(2, 20 + r.nextInt(61), Timestamp.valueOf("2022-01-02 10:01:00")),
      SimpleSensorWithTimestamp(3, 20 + r.nextInt(61), Timestamp.valueOf("2022-01-02 10:02:00")),
      // a late data!
      SimpleSensorWithTimestamp(4, 20 + r.nextInt(61), Timestamp.valueOf("2022-01-01 09:04:00")),
      SimpleSensorWithTimestamp(3, 20 + r.nextInt(61), Timestamp.valueOf("2022-01-01 09:05:00")),
      SimpleSensorWithTimestamp(4, 20 + r.nextInt(61), Timestamp.valueOf("2022-01-01 09:06:00")),
      SimpleSensorWithTimestamp(4, 20 + r.nextInt(61), Timestamp.valueOf("2022-01-01 09:07:00")),
      SimpleSensorWithTimestamp(4, 20 + r.nextInt(61), Timestamp.valueOf("2022-01-01 09:08:00")),
      SimpleSensorWithTimestamp(4, 20 + r.nextInt(61), Timestamp.valueOf("2022-01-01 09:09:00")),
      SimpleSensorWithTimestamp(4, 20 + r.nextInt(61), Timestamp.valueOf("2022-01-01 09:10:00")),
      SimpleSensorWithTimestamp(4, 20 + r.nextInt(61), Timestamp.valueOf("2022-01-01 09:11:00")),
    )

    testData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }
  }
}
