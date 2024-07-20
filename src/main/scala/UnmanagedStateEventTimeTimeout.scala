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
  case class UserAction(userId: String, action: String, eventTimestamp: Timestamp)

  // Represents the status of a user
  case class UserStatus(userId: String, var active: Boolean) {
    /**
     * Updates the user's active status based on the action.
     * @param action
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
        updateUserStatusWithProcessingTimeTimeout _)

    // Start the streaming query and print to console
    val query = latestStatuses
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()

    // Wait for the data adding to finish (it won't, but in a real
    // use case you might want to manage this better)
    Await.result(addDataFuture, Duration.Inf)

  }

  def updateUserStatusWithProcessingTimeTimeout(userId: String,
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
   * Add such sampleData to a memoryStream so that Event Time Timeouts gets to work!
   * @param memoryStream
   * @param interval
   * @return
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
