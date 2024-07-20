package learningSpark

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
 * In the preceding example of tracking active user sessions, as
 * more users become active, the number of keys in the state
 * will keep increasing, and so will the memory used by the state.
 *
 * Now, in a real-world scenario, users are likely not going to stay
 * active all the time.
 *
 * It may not be very useful to keep the status of inactive users in the
 * state, as it is not going to change again until those users
 * become active again. Hence, we may want to explicitly drop all
 * information for inactive users.
 *
 * However, a user may not explicitly take any action to become
 * inactive (e.g., explicitly logging off), and we may have to define inactivity
 * as lack of any action for a threshold duration.
 *
 * This becomes tricky to encode in the function, as the function is
 * not called for a user until there are new actions from that user.
 *
 * We have timeouts for this, and this application demonstrates processing
 * time timeouts.
 */
object UnmanagedStateProcessingTimeTimeout {
  // let's scope the case classes we will use to this object

  // Represents a user action
  case class UserAction(userId: String, action: String)

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
      .appName("Unmanaged State Processing Time Timeouts")
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
      .groupByKey(userAction => userAction.userId)
      .mapGroupsWithState(
        GroupStateTimeout.ProcessingTimeTimeout)(
        updateUserStatusWithProcessingTimeTimeout _)

    // Start the streaming query and print to console
    val query = latestStatuses
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    // TODO: is this a valid approach?
    // Add shutdown hook to handle SIGINT gracefully
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        query.stop()
        spark.stop()
      }
    })

    // If we somehow do not stop the query,
    // every user will be timed out eventually, and the
    // stream will keep going.
    // So this scheduler will crash the app but that's what we want :)
    // Schedule sending of SIGINT after 30 seconds
    val scheduler: ScheduledExecutorService = Executors
      .newScheduledThreadPool(1)

    scheduler.schedule(new Runnable {
      def run(): Unit = {
        println("Sending SIGINT to stop the streaming query")
        sys.runtime.exit(2) // Simulate sending SIGINT
      }
    }, 30, TimeUnit.SECONDS)

    query.awaitTermination()

    // Wait for the data adding to finish (it won't, but in a real
    // use case you might want to manage this better)
    Await.result(addDataFuture, Duration.Inf)
  }

  /**
   * A State Update Function with Processing Time Timeout.
   *
   * It can be used with mapGroupsWithState
   * @param userId
   * @param newActions
   * @param state
   * @return
   */
  def updateUserStatusWithProcessingTimeTimeout(userId: String,
                       newActions: Iterator[UserAction],
                       state: GroupState[UserStatus]): UserStatus = {
    if (!state.hasTimedOut) { // Was not called due to timeout
      val userStatus = state.getOption.getOrElse(UserStatus(userId, false))
      newActions.foreach { action => userStatus.updateWith(action) }
      state.update(userStatus)
      state.setTimeoutDuration("10 seconds") // Set timeout duration
      userStatus

    } else {
      val userStatus = state.get
      // Remove state when timed out
      state.remove()
      // Return inactive user's status
      userStatus.asInactive()
    }
  }

  /**
   * This method adds the sample data within it to a MemoryStream.
   *
   * @param memoryStream
   * @param interval
   * @return
   */
  def addDataPeriodicallyToUserActionMemoryStream(memoryStream: MemoryStream[UserAction],
                                                  interval: FiniteDuration): Future[Unit] = Future {

    // we will never send data from user 2 again
    // so it should be dropped first!
    val sampleData: Seq[UserAction] = Seq(
      UserAction("1", "move"),
      UserAction("2", "move"),
      UserAction("3", "move"),
      // user 1 become idle and user 2 - 3 is moving
      UserAction("1", "idle"),
      UserAction("4", "move"),
      UserAction("3", "move"),
    )

    sampleData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }
  }
}
