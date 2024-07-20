package learningSpark

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration

/**
 * Let's say we have some events that should be happening uniquely
 * */
case class Event(guid: String, eventTime: Timestamp, data: String)

/** We can deduplicate records in data streams using
 * a unique identifier in the events.
 *
 * Let's see an example application on Events.
 */
object StreamingDeduplication {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Streaming Deduplication")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Initialize MemoryStream
    val eventMemoryStream = new MemoryStream[Event](1, spark.sqlContext)

    // Add sample data every second - one by one
    val addDataFuture = addDataPeriodicallyToMemoryStream(
      eventMemoryStream, 1.seconds)

    val eventStream: DataFrame = eventMemoryStream
      .toDF()

    // Deduplication without watermarking
    val deduplicatedWithoutWatermark = eventStream
      .dropDuplicates("guid")

    // TODO: this does not work ?
    // Deduplicate using both the guid and the event time columns.
    // This bounds the amount of the state the query has to maintain.
    // val deduplicatedWithWatermark = eventStream
    //   // if there is a duplication it will
    //   // at most happen in 2 hours
    //   // after 2 hours you can remove old state.
    //   .withWatermark("eventTime", "10 hours")
    //   .dropDuplicates("guid", "eventTime")

    // Start the streaming query and print to console
    val queryWithoutWatermark = deduplicatedWithoutWatermark
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    // TODO: this does not work ?
    // val queryWithWatermark = deduplicatedWithWatermark
    //   .writeStream
    //   .outputMode("append")
    //   .format("console")
    //   .option("truncate", false)
    //   .trigger(Trigger.ProcessingTime("5 seconds"))
    //   .start()

    queryWithoutWatermark.awaitTermination()
    // queryWithWatermark.awaitTermination()

    // Wait for the data adding to finish (it won't, but in a real
    // use case you might want to manage this better)
    Await.result(addDataFuture, Duration.Inf)
  }

  /**
   * Add generated data on time interval, to an existing MemoryStream.
   * This simulates a real streaming scenario where data arrives continuously.
   */
  def addDataPeriodicallyToMemoryStream(memoryStream: MemoryStream[Event],
                                        interval: FiniteDuration): Future[Unit] = Future {

    val sampleData: Seq[Event] = Seq(
      Event("862207ec-14d1-4671-9e61-56820f1a8b14", Timestamp.valueOf("2023-06-09 12:00:00"), "data1"),
      Event("8562e978-54fd-4fc5-b70c-2b8f7e1346b5", Timestamp.valueOf("2023-06-09 12:11:00"), "data2"),
      Event("0f1d9d2f-9044-45fb-a744-dd7312a0c4f1", Timestamp.valueOf("2023-06-09 12:22:00"), "data3"),
      Event("862207ec-14d1-4671-9e61-56820f1a8b14", Timestamp.valueOf("2023-06-09 12:33:00"), "data1_duplicate"),
      Event("8562e978-54fd-4fc5-b70c-2b8f7e1346b5", Timestamp.valueOf("2023-06-09 12:44:00"), "data2_duplicate"),
      Event("aaa5e41e-8127-4fc2-a93d-8c15f3aa3e42", Timestamp.valueOf("2023-06-09 12:55:00"), "data4"),
      Event("a4ad4480-d39b-495f-8940-31d2d148b29c", Timestamp.valueOf("2023-06-09 13:16:00"), "data5"),
      Event("862207ec-14d1-4671-9e61-56820f1a8b14", Timestamp.valueOf("2023-06-09 13:27:00"), "data1_duplicate2"),
      Event("8562e978-54fd-4fc5-b70c-2b8f7e1346b5", Timestamp.valueOf("2023-06-09 13:38:00"), "data2_duplicate2"),
      Event("54b5d173-8c54-4a34-87f4-d8bf7c312b08", Timestamp.valueOf("2023-06-09 13:49:00"), "data6")
    )

    sampleData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }
  }
}
