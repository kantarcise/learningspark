package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession, SQLContext}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, Trigger}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite

import org.scalatest.matchers.should.Matchers._

class UnmanagedStateWithFlatMapGroupsWithStateTest extends AnyFunSuite with Eventually {

  test("User alerts should be generated correctly based on user actions") {
    // Initialize SparkSession
    implicit val spark: SparkSession = SparkSession
      .builder
      .appName("FlatMapGroupsWithState Test")
      .master("local[*]")
      .getOrCreate()

    // Import implicits for Dataset conversions
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Initialize MemoryStream with implicit SQLContext
    val userActionMemoryStream =
      MemoryStream[UnmanagedStateWithFlatMapGroupsWithState.UserAction](1, spark.sqlContext)

    // Define batches of data
    val batches = Seq(
      Seq(
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("1", "move"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("2", "move"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("3", "move")
      ),
      Seq(
        // Should generate alert for user 1
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("1", "idle"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("4", "move"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("3", "move")
      ),
      Seq(
        // User 1 becomes active again
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("1", "move"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("3", "move"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("4", "move"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("5", "move")
      ),
      Seq(
        // Set all users to idle, should generate alerts for users 2, 3, 4, 5
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("2", "idle"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("3", "idle"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("4", "idle"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAction("5", "idle")
      )
    )

    // Get user actions Dataset
    val userActions: Dataset[UnmanagedStateWithFlatMapGroupsWithState.UserAction] =
      userActionMemoryStream.toDS()

    // Process user actions to generate alerts
    val userAlerts = userActions
      .groupByKey(_.userId)
      .flatMapGroupsWithState(
        outputMode = OutputMode.Append,
        timeoutConf = GroupStateTimeout.NoTimeout
      )(UnmanagedStateWithFlatMapGroupsWithState.getUserAlerts)

    // Start the streaming query and collect results to memory sink
    val query = userAlerts
      .writeStream
      .format("memory")
      .outputMode("append")
      .queryName("userAlerts")
      .trigger(Trigger.ProcessingTime("1 second"))
      .start()

    try {
      // Process each batch
      for (batch <- batches) {
        userActionMemoryStream.addData(batch: _*)
        query.processAllAvailable()
      }

      // Collect the results
      val result = spark.sql("SELECT * FROM userAlerts")

      // Convert DataFrame to Dataset[UserAlert]
      val alerts = result.as[(String, String)].collect().map {
        case (userId, message) =>
          UnmanagedStateWithFlatMapGroupsWithState.UserAlert(userId, message)
      }

      // Expected alerts
      val expectedAlerts = Seq(
        UnmanagedStateWithFlatMapGroupsWithState.UserAlert("1", "User is inactive"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAlert("2", "User is inactive"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAlert("3", "User is inactive"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAlert("4", "User is inactive"),
        UnmanagedStateWithFlatMapGroupsWithState.UserAlert("5", "User is inactive")
      )

      // Assertions
      alerts.toSet shouldBe expectedAlerts.toSet

      // Optionally, display the result for verification
      result.show()
    } finally {
      // Stop the query and SparkSession
      query.stop()
      spark.stop()
    }
  }
}
