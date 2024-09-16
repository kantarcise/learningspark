package learningSpark

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

/**
 * In this example, we will learn about how we can manage
 * the state ourselves, with help of
 * mapGroupsWithState()
 *
 * For more details, check page 254.
 */
object UnmanagedStatefulOperations {

  // Represents a user action
  case class UserAction(userId: String, action: String)

  // Represents the status of a user
  case class UserStatus(userId: String, var active: Boolean) {
    /**
     * Updates the user's active status based on the action.
     * @param action: The UserAction
     */
    def updateWith(action: UserAction): Unit = {
      active = action.action match {
        case "move" => true
        case "idle" => false
        case _      => active
      }
    }
  }

  def main(args: Array[String]): Unit = {
    // let's try to decompose the code
    implicit val spark: SparkSession = createSparkSession()

    spark.sparkContext.setLogLevel("WARN")

    val userActionMemoryStream = initializeMemoryStream()
    val addDataFuture = addDataPeriodically(userActionMemoryStream, 1.second)

    val userActions = userActionMemoryStream.toDS()
    val latestStatuses = computeLatestStatuses(userActions)
    val query = startStreamingQuery(latestStatuses)

    query.awaitTermination()
    Await.result(addDataFuture, Duration.Inf)
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder
      .appName("Unmanaged Stateful Operations")
      .master("local[*]")
      .getOrCreate()
  }

  /**
   *  In Scala, a method can have parameters marked as implicit.
   *  These parameters do not need to be passed explicitly
   *  when calling the method, provided there is an implicit
   *  value of the required type in the scope where
   *  the method is called.
   *
   * The method has two parameter lists:
   *    The first is empty (), indicating no explicit parameters.
   *    The second (implicit spark: SparkSession) declares an implicit
   *        parameter of type SparkSession.
   *
   * Benefits of Using Implicit Parameters:
   *    Reduces Boilerplate: Eliminates the need to pass the same
   *       parameter repeatedly to multiple methods.
   *
   *    Improves Readability: Simplifies method calls and makes code cleaner.
   */
  def initializeMemoryStream()(implicit spark: SparkSession): MemoryStream[UserAction] = {
    import spark.implicits._
    MemoryStream[UserAction](1, spark.sqlContext)
  }

  /**
   * In our example
   * when we run this method for our input we'll see these
   * final UserStatus for each user:
   *
   * +------+------+
   * |userId|active|
   * +------+------+
   * |     1| false|
   * |     3|  true|
   * |     2|  true|
   *
   *
   * @param userActions: The UserAction Dataset
   * @return
   */
  def computeLatestStatuses(userActions: Dataset[UserAction]): Dataset[UserStatus] = {
    import userActions.sparkSession.implicits._

    userActions
      .groupByKey(_.userId)
      .mapGroupsWithState(updateUserStatus)
  }

  def startStreamingQuery(latestStatuses: Dataset[UserStatus]): StreamingQuery = {
    latestStatuses
      .writeStream
      .queryName("Latest Status of Users to Console")
      .outputMode("update")
      .format("console")
      .start()
  }

  def updateUserStatus(
                        userId: String,
                        newActions: Iterator[UserAction],
                        state: GroupState[UserStatus]
                      ): UserStatus = {
    // Get the current state or make a new state if none exists
    val userStatus = state
      .getOption
      .getOrElse(
        UserStatus(
          userId, active = false))
    // Update state with new actions
    newActions.foreach(userStatus.updateWith)

    // Update the state
    state.update(userStatus)
    userStatus
  }

  /**
   * Add generated data on time interval, to an existing MemoryStream.
   * This simulates a real streaming scenario where data arrives continuously.
   */
  def addDataPeriodically(memoryStream: MemoryStream[UserAction],
                          interval: FiniteDuration
                         ): Future[Unit] = Future {
    val sampleData = Seq(
      UserAction("1", "move"),
      UserAction("2", "idle"),
      UserAction("3", "move"),
      // user 1 become idle and user 2 - 3 is moving
      UserAction("1", "idle"),
      UserAction("2", "move"),
      UserAction("3", "move")
    )

    sampleData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }
  }
}
