package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import java.sql.Timestamp

/**
 * Instead of the system clock time, an event-time timeout is
 * based on the event time in the data (similar to time-based
 * aggregations) and a watermark defined on the event time.
 *
 * If a key is configured with a specific timeout timestamp
 * of T (i.e., not a duration), then that key will time out when
 * the watermark exceeds T if no new data was
 * received for that key since the last time the function was called.
 *
 * This is great, because unlike with processing-time timeouts any
 * slowdown or downtime in query processing will not cause spurious
 * timeouts.
 */
object UnmanagedStateEventTimeTimeout {
  // let's scope the case classes we will use to this object

  // Represents a user action
  case class UserAction(userId: String, action: String,
                        eventTimestamp: Timestamp)

  // Represents the status of a user
  case class UserStatus(userId: String, var active: Boolean) {
    /**
     * Updates the user's active status based on the action.
     * @param action: the UserAction to base the status on.
     */
    def updateWith(action: UserAction): Unit = {
      active = action.action match {
        case "move" => true
        case "idle" => false
        case _ => active
      }
    }

    /**
     * Marks the user as inactive.
     * @return UserStatus with active set to false
     */
    def asInactive(): UserStatus = {
      this.active = false
      this
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Unmanaged State Event Time Timeouts")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Initialize MemoryStream
    val userActionMemoryStream = new
        MemoryStream[UserAction](1, spark.sqlContext)

    // Add sample data every second - one by one
    val addDataFuture = addDataPeriodicallyToUserActionMemoryStream(
      userActionMemoryStream,
      2.seconds)

    val userActions: Dataset[UserAction] = userActionMemoryStream
      .toDS()

    val latestStatuses = userActions
      .withWatermark("eventTimestamp", "10 minutes")
      .groupByKey(userAction => userAction.userId)
      .mapGroupsWithState(
        GroupStateTimeout.EventTimeTimeout)(
        updateUserStatusWithEventTimeTimeout _)

    // Start the streaming query and print to console
    val query = latestStatuses
      .writeStream
      .queryName("Latest Status of Users to Console")
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()

    // Wait for the data adding to finish (it won't, but in a real
    // use case you might want to manage this better)
    Await.result(addDataFuture, Duration.Inf)
  }


  /**
   * Updates the user status based on new actions
   * and manages state with event time timeouts.
   *
   * This function is called for each unique user ID in
   * the micro-batch’s data. It updates the user status based
   * on the new actions received and sets an event time timeout to manage state.
   *
   * If the function is called due to a timeout, it marks
   * the user as inactive and removes the state.
   *
   * Otherwise, it updates the user's status based on the
   * new actions and sets the timeout timestamp
   * to the current watermark + 1 hour.
   *
   * @param userId The unique identifier for the user.
   * @param newActions An iterator of new actions for the
   *                   user in the current micro-batch.
   * @param state The state corresponding to the user, which
   *              includes their status and timeout information.
   * @return The updated user status.
   */
  def updateUserStatusWithEventTimeTimeout(userId: String,
                                           newActions: Iterator[UserAction],
                                           state: GroupState[UserStatus]): UserStatus = {
    if (!state.hasTimedOut) { // Was not called due to timeout
      val userStatus = state.getOption.getOrElse(UserStatus(userId, false))
      newActions.foreach { action => userStatus.updateWith(action) }
      state.update(userStatus)

      // Set the timeout timestamp to the current watermark + 1 hour
      state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + 3600000)
      userStatus
    } else {
      val userStatus = state.get
      state.remove()
      userStatus.asInactive()
    }
  }

  /**
   * Adds sample data to a memory stream at regular
   * intervals to simulate a stream of user actions.
   *
   * This method adds predefined user actions to the
   * given MemoryStream at the specified interval.
   *
   * The sample data is designed to demonstrate the
   * concept of event-time timeouts.
   *
   * Specifically:
   * - User 2 moves initially but has no further actions, causing
   *      it to be dropped due to inactivity.
   * - User 5 moves at the end, which causes previous
   *      users to be dropped due to inactivity.
   *
   * @param memoryStream The MemoryStream to which
   *                     the sample data will be added.
   * @param interval The interval between each
   *                 data addition to the MemoryStream.
   * @return A Future[Unit] that completes once all the
   *         data has been added to the MemoryStream.
   */
  def addDataPeriodicallyToUserActionMemoryStream(memoryStream: MemoryStream[UserAction],
                                                  interval: FiniteDuration): Future[Unit]  = Future {

    // user 1 went idle, so it will be false by activity
    // user 2 will be dropped do to inactivity
    val sampleData: Seq[UserAction] = Seq(
      UserAction("1", "move", Timestamp.valueOf("2023-06-09 12:00:00")),
      UserAction("2", "move", Timestamp.valueOf("2023-06-09 12:05:00")),
      UserAction("3", "move", Timestamp.valueOf("2023-06-09 12:20:00")),
      UserAction("1", "idle", Timestamp.valueOf("2023-06-09 12:35:00")),
      UserAction("4", "move", Timestamp.valueOf("2023-06-09 12:45:00")),
      UserAction("3", "move", Timestamp.valueOf("2023-06-09 12:50:00")),
      UserAction("1", "move", Timestamp.valueOf("2023-06-09 13:10:00")),
      UserAction("3", "move", Timestamp.valueOf("2023-06-09 13:20:00")),
      UserAction("4", "move", Timestamp.valueOf("2023-06-09 13:30:00")),
      // this last user will make 1, 3 and 4 drop, due to inactivity
      UserAction("5", "move", Timestamp.valueOf("2023-06-09 14:30:00")),
    )

    sampleData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }
  }
}
