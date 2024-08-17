package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupStateTimeout, Trigger}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}
import UnmanagedStateEventTimeTimeout.updateUserStatusWithEventTimeTimeout

import scala.concurrent.duration._
import scala.concurrent.Await

import java.sql.Timestamp

// TODO: what are these?
// Here is a guide:
// https://www.scalatest.org/user_guide/using_matchers
import org.scalatest.matchers.should._
import org.scalatest.flatspec._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class UnmanagedStateEventTimeTimeoutTest extends AnyFunSuite with Eventually {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("Unmanaged State Event Time Timeouts Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  test("User status should be updated correctly based on actions and event time timeouts") {
    // Initialize MemoryStream
    val userActionMemoryStream = new
        MemoryStream[UnmanagedStateEventTimeTimeout.UserAction](1,
          spark.sqlContext)

    // Add sample data every second - one by one
    val addDataFuture = UnmanagedStateEventTimeTimeout
      .addDataPeriodicallyToUserActionMemoryStream(userActionMemoryStream,
        1.seconds)

    // let's use the scoped case class
    val userActions: Dataset[UnmanagedStateEventTimeTimeout.UserAction] = userActionMemoryStream
      .toDS()

    val latestStatuses = userActions
      .withWatermark("eventTimestamp", "10 minutes")
      .groupByKey(userAction => userAction.userId)
      .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout)(
        updateUserStatusWithEventTimeTimeout)

    // Start the streaming query and collect results to memory
    val query = latestStatuses
      .writeStream
      .outputMode("update")
      .format("memory")
      .queryName("latestStatuses")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    try {
      // Wait for the data adding to finish
      Await.result(addDataFuture, Duration.Inf)

      // Use eventually to wait until the stream processing completes
      eventually(timeout(Span(60, Seconds)), interval(Span(6, Seconds))) {
        val result = spark.sql("SELECT * FROM latestStatuses")

        // Convert DataFrame to Dataset[UserStatus]
        val statuses = result
          .as[(String, Boolean)]
          .map { case (userId, active) => (userId,
            UnmanagedStateEventTimeTimeout.UserStatus(userId, active)) }
          .collect()
          .toMap

        // Because of the EventTime Timeout, every users should
        // be inactive in the end
        assert(!statuses("1").active)
        assert(!statuses("2").active)
        assert(!statuses("3").active)
        assert(!statuses("4").active)
        assert(statuses("5").active == false)
        result.show()
      }
    } finally {
      query.stop()
    }
  }
}
