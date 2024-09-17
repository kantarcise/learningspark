package learningSpark

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
 * In the preceding example (UnmanagedStatefulOperations) of tracking
 * active user sessions, as more users become active, the number of
 * keys in the state will keep increasing, and so will the memory
 * used by the state.
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
     * @param action The `UserAction` instance containing the user's action.
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
     * @return The updated `UserStatus` with `active` set to `false`.
     */
    def asInactive(): UserStatus = {
      this.active = false
      this
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createSparkSession()
    spark.sparkContext.setLogLevel("WARN")

    val userActionMemoryStream = initializeMemoryStream()
    val addDataFuture = addDataPeriodically(userActionMemoryStream, 2.seconds)

    val userActions = userActionMemoryStream.toDS()
    val latestStatuses = computeLatestStatuses(userActions)
    val query = startStreamingQuery(latestStatuses)

    setupGracefulShutdown(query, spark)
    scheduleApplicationTermination(30.seconds)

    query.awaitTermination()
    Await.result(addDataFuture, Duration.Inf)
  }

  /**
   * Makes a SparkSession with the application name
   * and master configuration.
   *
   * @return An implicit `SparkSession` instance.
   */
  def createSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("Unmanaged State Processing Time Timeouts")
      .master("local[*]")
      .getOrCreate()
  }

  /**
   * Initializes a `MemoryStream` for `UserAction` data.
   *
   * @param spark An implicit `SparkSession`.
   * @return A `MemoryStream` of `UserAction`.
   */
  def initializeMemoryStream()(
    implicit spark: SparkSession): MemoryStream[UserAction] = {
    import spark.implicits._
    MemoryStream[UserAction](1, spark.sqlContext)
  }

  /**
   * State update function with processing time timeout for `mapGroupsWithState`.
   *
   * @param userId     The key (user ID) for the group.
   * @param newActions An iterator of `UserAction` for the group.
   * @param state      The `GroupState` maintaining the `UserStatus`.
   * @return The updated `UserStatus` for the user.
   */
  def updateUserStatusWithProcessingTimeTimeout(userId: String,
                       newActions: Iterator[UserAction],
                       state: GroupState[UserStatus]): UserStatus = {
    if (!state.hasTimedOut) { // Was not called due to timeout
      val userStatus = state.getOption.getOrElse(UserStatus(userId, active = false))
      newActions.foreach { action => userStatus.updateWith(action) }
      state.update(userStatus)
      state.setTimeoutDuration("10 seconds") // Set timeout duration
      userStatus

    } else {
      // get Throws a NoSuchElementException if the state is not set.
      // val userStatus = state.get
      // sp let's use this
      val userStatus = state.getOption.getOrElse(UserStatus(userId, active = false))
      // Remove state when timed out
      state.remove()
      // Return inactive user's status
      userStatus.asInactive()
    }
  }

  /**
   * Adds sample `UserAction` data to the
   * provided `MemoryStream` at specified intervals.
   *
   * @param memoryStream The `MemoryStream` to add data to.
   * @param interval     The interval between data additions.
   * @return A `Future` representing the asynchronous data addition task.
   */
  def addDataPeriodically(memoryStream: MemoryStream[UserAction],
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

  /**
   * Computes the latest user statuses using `mapGroupsWithState` with processing time timeout.
   *
   * @param userActions The input `Dataset` of `UserAction`.
   * @return A `Dataset` of `UserStatus` representing the latest statuses.
   */
  def computeLatestStatuses(userActions: Dataset[UserAction]): Dataset[UserStatus] = {
    import userActions.sparkSession.implicits._

    userActions
      .groupByKey(_.userId)
      .mapGroupsWithState(
        GroupStateTimeout.ProcessingTimeTimeout)(
        updateUserStatusWithProcessingTimeTimeout)
  }

  /**
   * Starts the streaming query to write the latest user statuses to the console.
   *
   * @param latestStatuses The `Dataset` of `UserStatus` to be written.
   * @return A `StreamingQuery` object representing the active streaming query.
   */
  def startStreamingQuery(latestStatuses: Dataset[UserStatus]): StreamingQuery = {
    latestStatuses
      .writeStream
      .queryName("Latest States as they are Updated to Console")
      .outputMode("update")
      .format("console")
      .start()
  }

  /**
   * Sets up graceful shutdown of the streaming query and SparkSession on application termination.
   *
   * @param query The active `StreamingQuery`.
   * @param spark The active `SparkSession`.
   */
  def setupGracefulShutdown(query: StreamingQuery, spark: SparkSession): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        println("Gracefully stopping the streaming query and SparkSession...")
        query.stop()
        spark.stop()
      }
    })
  }

  /**
   * Schedules the application to terminate after a specified duration.
   *
   * @param delay The delay after which the application will terminate.
   */
  def scheduleApplicationTermination(delay: FiniteDuration): Unit = {
    val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
    scheduler.schedule(new Runnable {
      def run(): Unit = {
        println(s"Application will terminate after $delay.")
        sys.exit(0)
      }
    }, delay.toSeconds, TimeUnit.SECONDS)
  }

  def mainNotDecomposed(args: Array[String]): Unit = {

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
    val addDataFuture = addDataPeriodically(
      userActionMemoryStream,
      2.seconds)

    val userActions: Dataset[UserAction] = userActionMemoryStream
      .toDS()

    val latestStatuses = userActions
      .groupByKey(userAction => userAction.userId)
      .mapGroupsWithState(
        GroupStateTimeout.ProcessingTimeTimeout)(
        updateUserStatusWithProcessingTimeTimeout)

    // Start the streaming query and print to console
    val query = latestStatuses
      .writeStream
      .queryName("Latest States as they are Updated to Console")
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
}
