package learningSpark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel._

import scala.util.Random

/**
 * Make sure you delete the folders inside
 * spark-warehouse before running the app.
 *
 * Can we optimize SortMergeJoin ?
 *
 * Yes!
 *
 * We can eliminate the Exchange step from the previous example
 * if we made partitioned buckets for common sorted
 * keys or columns on which we want to perform frequent
 * equijoins.
 *
 * We can make an explicit number of buckets to store
 * specific sorted columns (one key per bucket). Presorting and
 * reorganizing data in this way boosts performance, as
 * it allows us to skip the expensive Exchange
 * operation and go straight to WholeStageCodegen.
 */
object SortMergeJoinBucketed {

  // a function to benchmark any code or function
  def benchmark(name: String)(f: => Unit) {
    val startTime = System.nanoTime
    f
    val endTime = System.nanoTime
    println(s"Time taken in $name: " +
      (endTime - startTime).toDouble / 1000000000 +
      " seconds")
  }

  /**
   * Make a Spark Session which is configured to use
   * SortMergeJoin.
   * @return
   */
  def createSparkSession(): SparkSession = {
    SparkSession.builder
      .appName("SortMergeJoinBucketed")
      .config("spark.sql.join.preferSortMergeJoin", value = true)
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.shuffle.partitions", 24)
      .master("local[*]")
      .getOrCreate()
  }

  /**
   * Generate two different maps and return them.
   *
   * @return
   */
  def initializeData(): (Map[Int, String], Map[Int, String]) = {
    // we can make empty Maps than append keys to them!
    val states = scala.collection.mutable.Map[Int, String]()
    val items = scala.collection.mutable.Map[Int, String]()
    states += (0 -> "AZ", 1 -> "CO", 2 -> "CA",
      3 -> "TX", 4 -> "NY", 5 -> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2 -> "SKU-2",
      3 -> "SKU-3", 4 -> "SKU-4", 5 -> "SKU-5")
    (states.toMap, items.toMap)
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
  def createDataFrames(spark: SparkSession,
                       states: Map[Int, String],
                       items: Map[Int, String],
                       rnd: Random) = {
    import spark.implicits._

    val usersDF = (0 to 100000)
      .map(id =>
        (id,
          s"user_${id}",
          s"user_${id}@dreamers.com",
          states(rnd.nextInt(5))))
      .toDF("uid", "login", "email", "user_state")

    val ordersDF = (0 to 100000)
      .map(r =>
        (r,
          r,
          rnd.nextInt(100000),
          10 * r * 0.2d,
          states(rnd.nextInt(5)),
          items(rnd.nextInt(5))))
      .toDF("transaction_id", "quantity", "users_id",
        "amount", "state", "items")

    (usersDF, ordersDF)
  }

  /**
   * Persist given dataframes to Disk.
   * @param usersDF: users Dataframe
   * @param ordersDF: orders Dataframe
   */
  def persistDataFrames(usersDF: DataFrame, ordersDF: DataFrame): Unit = {
    usersDF.persist(DISK_ONLY)
    ordersDF.persist(DISK_ONLY)
  }

  /**
   * Use bucketing on the dataframes and save
   * them as Tables.
   * @param spark: the SparkSession
   * @param usersDF: the users Dataframe
   * @param ordersDF: orders Dataframe
   */
  def createBuckets(spark: SparkSession,
                    usersDF: DataFrame,
                    ordersDF: DataFrame): Unit = {
    spark.sql("DROP TABLE IF EXISTS UsersTbl")
    usersDF.orderBy(asc("uid"))
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .bucketBy(12, "uid")
      .saveAsTable("UsersTbl")

    spark.sql("DROP TABLE IF EXISTS OrdersTbl")
    ordersDF.orderBy(asc("users_id"))
      .write.format("parquet")
      .bucketBy(8, "users_id")
      .mode(SaveMode.Overwrite)
      .saveAsTable("OrdersTbl")

    spark.sql("CACHE TABLE UsersTbl")
    spark.sql("CACHE TABLE OrdersTbl")
  }

  /**
   * Join two data Dataframes based on user id key!
   *
   * @param spark: the SparkSession
   * @return
   */
  def joinDataFrames(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val usersBucketDF = spark.table("UsersTbl")
    val ordersBucketDF = spark.table("OrdersTbl")

    val joinUsersOrdersBucketDF = ordersBucketDF
      .join(usersBucketDF, $"users_id" === $"uid")

    joinUsersOrdersBucketDF
  }

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    spark.sparkContext.setLogLevel("ERROR")

    val (states, items) = initializeData()

    val rnd = new Random(42)
    val (usersDF, ordersDF) = createDataFrames(spark, states, items, rnd)

    persistDataFrames(usersDF, ordersDF)
    createBuckets(spark, usersDF, ordersDF)

    val joinUsersOrdersBucketDF = joinDataFrames(spark)

    println("\nJoined Dataframe made after bucketing!\n")
    joinUsersOrdersBucketDF.show(false)

    joinUsersOrdersBucketDF.explain()

    Thread.sleep(1000 * 30 * 1)
  }
}
