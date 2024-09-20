package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.apache.spark.sql.streaming.StreamingQuery

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

  // Represents a user action
  case class UserAction(userId: String, action: String)

  // Represents the status of a user
  case class UserStatus(userId: String, var active: Boolean) {
    /**
     * Updates the user's active status based on the action.
     * @param action The `UserAction` to update the status with.
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
     * @return A sequence of `UserAlert` objects.
     */
    def generateAlerts(): Seq[UserAlert] = {
      if (!active) Seq(UserAlert(userId, "User is inactive")) else Seq()
    }
  }

  // Represents an alert for a user
  case class UserAlert(userId: String, message: String)

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createSparkSession()

    spark.sparkContext.setLogLevel("WARN")

    val userActionMemoryStream = initializeMemoryStream()
    val addDataFuture = addDataPeriodically(userActionMemoryStream, 2.seconds)

    val userActions = userActionMemoryStream.toDS()
    val userAlerts = processUserActions(userActions)
    val query = startStreamingQuery(userAlerts)

    query.awaitTermination()
    Await.result(addDataFuture, Duration.Inf)
  }

  /**
   * Makes a SparkSession with the application
   * name and master configuration.
   *
   * @return An implicit `SparkSession` instance.
   */
  def createSparkSession(): SparkSession = {
    SparkSession.builder
      .appName("FlatMapGroupsWithState Example")
      .master("local[*]")
      .getOrCreate()
  }

  /**
   * Initializes a `MemoryStream` for `UserAction` data.
   *
   * @param spark An implicit `SparkSession`.
   * @return A `MemoryStream` of `UserAction`.
   */
  def initializeMemoryStream()(implicit spark: SparkSession):
  MemoryStream[UserAction] = {
    import spark.implicits._
    new MemoryStream[UserAction](1, spark.sqlContext)
  }

  /**
   * Processes the user actions to generate alerts using `flatMapGroupsWithState`.
   *
   * @param userActions The input `Dataset` of `UserAction`.
   * @return A `Dataset` of `UserAlert` representing the alerts to be generated.
   */
  def processUserActions(userActions: Dataset[UserAction]): Dataset[UserAlert] = {
    import userActions.sparkSession.implicits._

    userActions
      .groupByKey(_.userId)
      .flatMapGroupsWithState(
        outputMode = OutputMode.Append,
        timeoutConf = GroupStateTimeout.NoTimeout
      )(getUserAlerts)
  }

  /**
   * Starts the streaming query to write user alerts to the console.
   *
   * @param userAlerts The `Dataset` of `UserAlert` to be written.
   * @return A `StreamingQuery` object representing the active streaming query.
   */
  def startStreamingQuery(userAlerts: Dataset[UserAlert]): StreamingQuery = {
    userAlerts
      .writeStream
      .queryName("User Alerts to Console")
      .outputMode("append")
      .format("console")
      .start()
  }

  /**
   * Processes user actions to update user status and generate alerts.
   *
   * This function is to be used with `flatMapGroupsWithState`.
   *
   * @param userId     The unique identifier for the user.
   * @param newActions An iterator of new actions for the user in the current micro-batch.
   * @param state      The state corresponding to the user, which includes their status.
   * @return An iterator of `UserAlert` objects.
   */
  def getUserAlerts(
                     userId: String,
                     newActions: Iterator[UserAction],
                     state: GroupState[UserStatus]
                   ): Iterator[UserAlert] = {
    // Retrieve or initialize user status
    val userStatus = state.getOption.getOrElse(UserStatus(userId, active = false))

    // Update user status with new actions
    newActions.foreach { action =>
      userStatus.updateWith(action)
    }

    // Update state
    state.update(userStatus)

    // Generate any number of alerts (can return zero or more records)
    userStatus.generateAlerts().iterator
  }

  /**
   * Adds sample data to a memory stream at regular intervals to simulate a stream of user actions.
   *
   * This method adds predefined user actions to the given `MemoryStream` at the specified interval.
   *
   * @param memoryStream The `MemoryStream` to which the sample data will be added.
   * @param interval     The interval between each data addition to the `MemoryStream`.
   * @return A `Future[Unit]` that completes once all the data has been added to the `MemoryStream`.
   */
  def addDataPeriodically(
                           memoryStream: MemoryStream[UserAction],
                           interval: FiniteDuration
                         ): Future[Unit] = Future {
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
      // Set all users to idle, by activity, not timeout
      UserAction("2", "idle"),
      UserAction("3", "idle"),
      UserAction("4", "idle"),
      UserAction("5", "idle")
    )

    sampleData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }
  }
}
