package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * Example object to demonstrate the usage of flatMapGroupsWithState.
 *
 * Which allows you to maintain and update state
 * across micro-batches. The state (in this case, UserStatus)
 * persists between micro-batches, meaning that as new data
 * arrives, it can be used to update the existing
 * state or make new state entries.
 */
object UnmanagedStateWithFlatMapGroupsWithState {

  // case classes
  case class UserAction(userId: String, action: String)

  // Represents the status of a user
  case class UserStatus(userId: String, var active: Boolean) {
    /**
     * Updates the user's active status based on the action.
     * @param action: The user action
     */
    def updateWith(action: UserAction): Unit = {
      active = action.action match {
        case "move" => true
        case "idle" => false
        case _ => active
      }
    }

    /**
     * Generates alerts based on the user's status.
     * @return A sequence of UserAlert objects
     */
    def generateAlerts(): Seq[UserAlert] = {
      if (!active) Seq(UserAlert(userId, "User is inactive")) else Seq()
    }
  }

  // Represents an alert for a user
  case class UserAlert(userId: String, message: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("FlatMapGroupsWithState Example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Initialize MemoryStream
    val userActionMemoryStream = new MemoryStream[UserAction](1,
      spark.sqlContext)

    // Add sample data every 2 seconds - one by one
    val addDataFuture = addDataPeriodicallyToUserActionMemoryStream(
      userActionMemoryStream, 2.seconds)

    val userActions: Dataset[UserAction] = userActionMemoryStream
      .toDS()

    // Process the user actions to generate alerts
    // using flatMapGroupsWithState
    val userAlerts = userActions
      .groupByKey(userAction => userAction.userId)
      .flatMapGroupsWithState(
        // Operator output mode: appending new records
        OutputMode.Append,
        // No timeout for state
        GroupStateTimeout.NoTimeout
        // Apply the function to process each group
      )(getUserAlerts)

    // Start the streaming query and print to console
    val query = userAlerts
      .writeStream
      .queryName("User Alerts to Console")
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()

    // Wait for the data adding to finish (it won't, but in
    // a real use case you might want to manage this better)
    Await.result(addDataFuture, Duration.Inf)
  }

  /** This is to be used with flatMapGroupsWithState
   * It processes user actions to update user status and generate alerts.
   *
   * @param userId The unique identifier for the user.
   * @param newActions An iterator of new actions for
   *                   the user in the current micro-batch.
   * @param state The state corresponding to the user, which
   *              includes their status and timeout information.
   * @return An iterator of UserAlert objects.
   */
  def getUserAlerts(userId: String,
                    newActions: Iterator[UserAction],
                    state: GroupState[UserStatus]): Iterator[UserAlert] = {
    // Retrieve or initialize user status
    val userStatus = state.getOption.getOrElse(UserStatus(userId, false))

    // Update user status with new actions
    newActions.foreach { action =>
      userStatus.updateWith(action)
    }

    // Update state
    state.update(userStatus)

    // Generate any number of alerts (can return zero or more records)
    userStatus.generateAlerts().toIterator
  }

  /**
   * Adds sample data to a memory stream at regular
   * intervals to simulate a stream of user actions.
   *
   * This method adds predefined user actions to the
   * given MemoryStream at the specified interval.
   *
   * @param memoryStream The MemoryStream to which
   *                     the sample data will be added.
   * @param interval The interval between each data
   *                 addition to the MemoryStream.
   * @return A Future[Unit] that completes once all the
   *         data has been added to the MemoryStream.
   */
  def addDataPeriodicallyToUserActionMemoryStream(memoryStream: MemoryStream[UserAction],
                                                  interval: FiniteDuration): Future[Unit] = Future {

    val sampleData: Seq[UserAction] = Seq(
      UserAction("1", "move"),
      UserAction("2", "move"),
      UserAction("3", "move"),
      UserAction("1", "idle"),
      UserAction("4", "move"),
      UserAction("3", "move"),
      UserAction("1", "move"),
      UserAction("3", "move"),
      UserAction("4", "move"),
      UserAction("5", "move"),
      // set all users to idle, by activity, not timeout
      UserAction("2", "idle"),
      UserAction("3", "idle"),
      UserAction("4", "idle"),
      UserAction("5", "idle"),
    )

    sampleData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }
  }
}
