package learningSpark

import org.apache.spark.sql.{SparkSession, DataFrame}
import scala.util.Random

/** This application is heavily inspired
 * from the books example.
 *
 * You can check out Page 190 in LearningSpark.
 */
object SortMergeJoin {

  /** To benchmark any code or function
   *
   * @param name: Name of method
   * @param f: method
   */
  def benchmark(name: String)(f: => Unit): Unit = {
    val startTime = System.nanoTime
    f
    val endTime = System.nanoTime
    println(s"Time taken in $name: " +
      (endTime - startTime).toDouble / 1000000000 +
      " seconds")
  }

  /**
   * Make a spark Session which is setup to use SortMergeJoin
   * @param appName: Application Name
   * @param master: The master selection
   * @return
   */
  def createSparkSession(appName: String, master: String): SparkSession = {
    SparkSession
      .builder
      .appName(appName)
      .config("spark.sql.join.preferSortMergeJoin", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.shuffle.partitions", 24)
      .master(master)
      .getOrCreate()
  }

  /**
   * Generate two different maps and return them.
   * @return
   */
  def initializeData(): (Map[Int, String], Map[Int, String]) = {
    val states = Map(0 -> "AZ", 1 -> "CO", 2 -> "CA",
      3 -> "TX", 4 -> "NY", 5 -> "MI")
    val items = Map(0 -> "SKU-0", 1 -> "SKU-1", 2 -> "SKU-2",
      3 -> "SKU-3", 4 -> "SKU-4", 5 -> "SKU-5")
    (states, items)
  }

  /**
   * Using the Random Value Generator,
   * generate two different Dataframes.
   *
   * @param spark: the SparkSession
   * @param states: State Codes
   * @param items: Different Items
   * @param rnd: the Random Value Generator
   * @return
   */
  def createDataFrame(spark: SparkSession,
                      states: Map[Int, String],
                      items: Map[Int, String],
                      rnd: Random): (DataFrame, DataFrame) = {
    import spark.implicits._

    val usersDF = (0 to 100000)
      .map(id =>
        (id,
          s"user_${id}",
          s"user_${id}@dreamers.com",
          states(rnd.nextInt(6))))
      .toDF("uid", "login", "email", "user_state")

    val ordersDF = (0 to 100000)
      .map(r =>
        (r,
          r,
          rnd.nextInt(100000),
          10 * r * 0.2d,
          states(rnd.nextInt(6)),
          items(rnd.nextInt(6))))
      .toDF("transaction_id", "quantity", "users_id",
        "amount", "state", "items")

    (usersDF, ordersDF)
  }

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession(
      "Learning More About Sort Merge Join", "local[*]")

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val (states, items) = initializeData()

    // let's check the configuration.
    println("\nspark.sql.shuffle.partitions is set to: " +
      spark.conf.get("spark.sql.shuffle.partitions"))
    println("spark.sql.join.preferSortMergeJoin is set to: " +
      spark.conf.get("spark.sql.join.preferSortMergeJoin"))

    val rnd = new Random(42)
    val (usersDF, ordersDF) = createDataFrame(spark, states, items, rnd)

    println("\nHere are our users and orders Dataframes:\n")
    usersDF.show(10)
    ordersDF.show(10)

    benchmark("Sort Merge Join") {
      val usersOrdersDF = ordersDF.join(usersDF, $"users_id" === $"uid")

      println("Result of the Sort Merge Join:\n")
      usersOrdersDF.show(truncate = false)
      usersOrdersDF.cache()

      println("Let's double check that we are using SortMergeJoin: \n")
      usersOrdersDF.explain()
    }

    // Uncomment to view the SparkUI
    // if you open the UI, IT shows three stages for the
    // entire job: the Exchange and Sort operations happen in the final stage
    // Thread.sleep(1000 * 60 * 3)

    spark.stop()
  }
}
