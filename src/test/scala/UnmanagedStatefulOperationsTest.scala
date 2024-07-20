package learningSpark

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._
import scala.concurrent.Await

class UnmanagedStatefulOperationsTest extends AnyFunSuite with Eventually  {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("Unmanaged Stateful Operations Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  test("User status should be updated correctly based on actions") {
    // Initialize MemoryStream
    val userActionMemoryStream = new MemoryStream[UserAction](1,
      spark.sqlContext)

    // Add sample data every second - one by one
    val addDataFuture = UnmanagedStatefulOperations
      .addDataPeriodicallyToUserActionMemoryStream(userActionMemoryStream,
        1.seconds)

    val userActions: Dataset[UserAction] = userActionMemoryStream
      .toDS()

    val latestStatuses = userActions
      .groupByKey(_.userId)
      .mapGroupsWithState(UnmanagedStatefulOperations.updateUserStatus _)

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
      eventually(timeout(30.seconds), interval(2.seconds)) {
        val result = spark.sql("SELECT * FROM latestStatuses")

        // Convert DataFrame to Dataset[UserStatus]
        val statuses = result
          .as[(String, Boolean)]
          .map { case (userId, active) =>
            (userId, UserStatus(userId, active)) }
          .collect()
          .toMap

        // in the end, 1 is false
        // 2 and 3 is true
        assert(statuses("1").active == false)
        // or
        // assert(!statuses("1").active)
        assert(statuses("2").active == true)
        // we can also write it this way:
        assert(statuses("3").active)

        // because we are in update output mode, we will
        // see all the changes
        result.show()
      }
    } finally {
      query.stop()
    }
  }
}
