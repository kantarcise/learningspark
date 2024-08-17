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

/** As the event time moves forward, new groups are automatically
made and their aggregates are automatically updated.
Late and out-of-order events get handled automatically, as
they simply update older groups.

Here, there is the problem of:

Indefinitely growing state size. :(

Meaning, the older groups continue to occupy the state
memory, waiting for any late data to update them.

Because the query does not know how late the information can be.

We solve that with Watermarks! :)
*/

/**
 * In many cases, rather than running aggregations over the
 * whole stream, you want aggregations over data bucketed by time windows.
 *
 * Continuing with our sensor example, say each sensor is expected
 * to send readings in regular intervals and we want to detect if any
 * sensor is reporting an unusually high number of times.
 *
 * We will use two rateStreams for this!
 *
 * Let's also use this opportunity to learn about RateStream!
 * https://jaceklaskowski.gitbooks.io/spark-structured-streaming/content/spark-sql-streaming-RateStreamSource.html
 */
object ManagedStatefulAggregationsWithTime {

  case class RateStream(timestamp: Timestamp, value: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Aggregations With Time")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // Make a DataFrame representing a RateStream
    val primaryRateStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      .as[RateStream]

    // Secondary rate stream simulating a
    // malfunctioning sensor (device 1)
    // with a rampUpTime of 10 seconds
    val malfunctioningRateStream = spark
      .readStream
      .format("rate")
      // Device 1 sends data 5 times per second
      .option("rowsPerSecond", 5)
      .option("rampUpTime", 10)
      .load()
      .as[RateStream]
      .map(seq => SimpleSensorWithTimestamp(1, 80, seq.timestamp))

    // val simpleSensorStream : Dataset[SimpleSensorWithTimestamp] =
    //   mapRateStreamToSensors(primaryRateStream)

    // Map both rate streams to sensor data
    val simpleSensorStream: Dataset[SimpleSensorWithTimestamp] =
      mapRateStreamToSensors(primaryRateStream)
        .union(malfunctioningRateStream)

    // Perform aggregation, tumbling window
    val aggregatedStreamTumbling: DataFrame = simpleSensorStream
      .groupBy(window($"eventTime", "5 minute"), $"device_id")
      .count()

    // Perform aggregation, sliding window
    val aggregatedStreamSliding: DataFrame = simpleSensorStream
      .groupBy(window(
        timeColumn = $"eventTime",
        windowDuration = "5 minute",
        slideDuration = "3 minute"), $"device_id")
      .count()

    // Start the streaming query and print to console
    val query: StreamingQuery = aggregatedStreamTumbling
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()

    val querySecond: StreamingQuery = aggregatedStreamSliding
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()

    query.awaitTermination()
    querySecond.awaitTermination()
  }

  /** Using a rate stream, map each batch
   * to SimpleSensorWithTimestamp
   *
   * @param rs:  The RateStream streaming Dataset
   * @return
   */
  def mapRateStreamToSensors(rs: Dataset[RateStream]
                            ): Dataset[SimpleSensorWithTimestamp] ={

    import rs.sparkSession.implicits._

    val start = 20
    val r = new scala.util.Random

    rs
      .map( seq =>
      SimpleSensorWithTimestamp(
        // sensors 1, 2, 3
        (seq.value % 3 + 1).toInt,
        start + r.nextInt(80),
        seq.timestamp)
    )
  }

  /**
   * Add generated data on time interval, to an existing MemoryStream.
   * This simulates a real streaming scenario where data arrives continuously.
   *
   * We can use a Memory stream an completely control the data.
   *
   * @param memoryStream: The memory stream to add the data
   * @param interval: the interval
   * @return
   */
  def addDataPeriodicallyToMemoryStream(memoryStream: MemoryStream[SimpleSensorWithTime],
                                        interval: FiniteDuration): Future[Unit] = Future {

    // Random temperatures - in between 20 and 80 celsius
    val start = 20
    val r = new scala.util.Random

    val testData: Seq[SimpleSensorWithTime] = Seq(
      SimpleSensorWithTime(1, start + r.nextInt(61), 1458444054),
      SimpleSensorWithTime(2, start + r.nextInt(80), 1458444054),
      SimpleSensorWithTime(3, start + r.nextInt(80), 1458444054),
      SimpleSensorWithTime(3, start + r.nextInt(80), 1458444084),
      SimpleSensorWithTime(1, start + r.nextInt(80), 1458444114),
      SimpleSensorWithTime(2, start + r.nextInt(80), 1458444114),
      SimpleSensorWithTime(3, start + r.nextInt(80), 1458444114),
      SimpleSensorWithTime(3, start + r.nextInt(80), 1458444144),
      SimpleSensorWithTime(1, start + r.nextInt(80), 1458444174),
      SimpleSensorWithTime(2, start + r.nextInt(80), 1458444174),
      SimpleSensorWithTime(3, start + r.nextInt(80), 1458444174),
      SimpleSensorWithTime(3, start + r.nextInt(80), 1458444204),
      SimpleSensorWithTime(1, start + r.nextInt(80), 1458444234),
      SimpleSensorWithTime(2, start + r.nextInt(80), 1458444234),
      SimpleSensorWithTime(3, start + r.nextInt(80), 1458444234),
      SimpleSensorWithTime(3, start + r.nextInt(80), 1458444264))

    testData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }
  }
}