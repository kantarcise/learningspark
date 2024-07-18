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
 * to send at most one reading per minute and we want to detect if any
 * sensor is reporting an unusually high number of times.
 */
object ManagedStatefulAggregationsWithTime {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Aggregations With Time")
      .master("local[*]")
      .getOrCreate()

    import  spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // Initialize MemoryStream
    val sensorMemoryStream = new
        MemoryStream[SimpleSensorWithTime](1, spark.sqlContext)

    // Add sample data every 3 seconds - one by one!
    val addDataFuture = addDataPeriodicallyToMemoryStream(
      sensorMemoryStream, 3.seconds)

    val sensorStreamWithTimeDS = sensorMemoryStream.toDS()

    // Convert the timestamp from Long to Timestamp using the map operation and new case class
    val sensorStreamWithTimestamp: Dataset[SimpleSensorWithTimestamp] = sensorStreamWithTimeDS
      .map { s => SimpleSensorWithTimestamp(
        s.device_id,
        s.temp,
        new Timestamp(s.timestamp * 1000))}

    // Perform aggregation, tumblingWindow
    // Our window will be {2016-03-20 05:20:00, 2016-03-20 05:25:00}
    val aggregatedStreamTumbling : DataFrame= sensorStreamWithTimestamp
      // five-minute windows as a dynamically computed grouping column.
      .groupBy(window($"eventTime", "5 minute"), $"device_id")
      .count()

    // perform aggregation, sliding window
    // Our window will be {2016-03-20 05:16:00, 2016-03-20 05:21:00}
    // to {2016-03-20 05:24:00, 2016-03-20 05:29:00} - 1 minute sliding
    // some events will be in multiple groups!
    val aggregatedStreamSliding : DataFrame = sensorStreamWithTimestamp
      // compute counts corresponding to 5-minute
      // windows sliding every 3 minutes,
      .groupBy(window(
        timeColumn =  $"eventTime",
        windowDuration =  "5 minute",
        slideDuration =  "3 minute"), $"device_id")
      .count

    // Start the streaming query and print to console
    val query = aggregatedStreamTumbling
      .writeStream
      .outputMode("complete")
      .format("console")
      // we can see the print out in full
      .option("truncate" , false)
      .start()

    val querySecond: StreamingQuery = aggregatedStreamSliding
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()

    // In final batch, we will see the that
    // sensor 3 is acting unnatural
    query.awaitTermination()
    querySecond.awaitTermination()

    // Wait for the data adding to finish (it won't, but in a real
    // use case you might want to manage this better)
    Await.result(addDataFuture, Duration.Inf)
  }

  /**
   Add generated data on time interval, to an existing MemoryStream.

   This simulates a real streaming scenario where data arrives continuously.
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
