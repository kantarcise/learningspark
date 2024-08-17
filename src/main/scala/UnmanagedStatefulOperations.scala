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
        case _ => active
      }
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
      1.seconds)

    val userActions: Dataset[UserAction] = userActionMemoryStream
      .toDS()

    // when we run this query, we will see these
    // final UserStatus for each user:
    /** +------+------+
     *  |userId|active|
     *  +------+------+
     *  |     1| false|
     *  |     3|  true|
     *  |     2|  true|
     */
    val latestStatuses = userActions
      // group by user id
      .groupByKey(userAction => userAction.userId)
      //.mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateUserStatus _)
      .mapGroupsWithState(updateUserStatus _)

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
   * When this streaming query is started, in each
   * micro-batch Spark will call this
   * updateUserStatus() for each unique key in
   * the micro-batch’s data.
   *
   * @param userId -  K
   * @param newActions -  Iterator[V]
   * @param state - GroupState[S]
   * @return
   */
  def updateUserStatus(userId: String,
                       newActions: Iterator[UserAction],
                       state: GroupState[UserStatus]): UserStatus = {
    // Get the current state or make a new state if none exists
    val userStatus = state
      .getOption
      .getOrElse(
        UserStatus(
          userId, false))

    // Update state with new actions
    newActions.foreach { action =>
      userStatus.updateWith(action)
    }
    // Update the state
    state.update(userStatus)
    userStatus
  }

  /**
   * Add generated data on time interval, to an existing MemoryStream.
   * This simulates a real streaming scenario where data arrives continuously.
   */
  def addDataPeriodicallyToUserActionMemoryStream(memoryStream: MemoryStream[UserAction],
                                                  interval: FiniteDuration): Future[Unit] = Future {

    val sampleData: Seq[UserAction] = Seq(
      UserAction("1", "move"),
      UserAction("2", "idle"),
      UserAction("3", "move"),
      // user 1 become idle and user 2 - 3 is moving
      UserAction("1", "idle"),
      UserAction("2", "move"),
      UserAction("3", "move"),
    )

    sampleData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }
  }
}
