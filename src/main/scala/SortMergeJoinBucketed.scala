package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.SaveMode
import scala.util.Random

// can we optimize SortMergeJoin ?

// Yes!

// We can eliminate the Exchange step from the previous example
// if we made partitioned buckets for common sorted keys
// or columns on which we want to perform frequent equijoins.

// We can make an explicit number of buckets to store specific sorted
// columns (one key per bucket). Presorting and reorganizing data
// in this way boosts performance, as it allows us to skip the
// expensive Exchange operation and go straight to WholeStageCodegen.
object SortMergeJoinBucketed {
  // curried function to benchmark any code or function
  def benchmark(name: String)(f: => Unit) {
    val startTime = System.nanoTime
    f
    val endTime = System.nanoTime
    println(s"Time taken in $name: " +
      (endTime - startTime).toDouble / 1000000000 +
      " seconds")
  }

  // main class setting the configs
  def main (args: Array[String] ) {

    val spark = SparkSession.builder
      .appName("SortMergeJoinBucketed")
      // default is true
      // .config("spark.sql.codegen.wholeStage", true)
      .config("spark.sql.join.preferSortMergeJoin", true)
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      // The default value is 200, for a 12 core machine
      .config("spark.sql.shuffle.partitions", 24)
      .master("local[*]")
      .getOrCreate ()

    import spark.implicits._

    // Verbose = false
    spark.sparkContext.setLogLevel("ERROR")

    var states = scala.collection.mutable.Map[Int, String]()
    var items = scala.collection.mutable.Map[Int, String]()
    // seed
    val rnd = new Random(42)

    // initialize states and items purchased
    states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4", 5-> "SKU-5")
    // create dataframes
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
          10 * r* 0.2d,
          states(rnd.nextInt(5)),
          items(rnd.nextInt(5))))
      .toDF("transaction_id", "quantity", "users_id",
        "amount", "state", "items")

    // cache them on Disk only so we can see the difference in size in the storage UI
    usersDF.persist(DISK_ONLY)
    ordersDF.persist(DISK_ONLY)

    // buckets are tables, which we will save in Parquet format.

    // let's make five buckets, each DataFrame for their respective columns

    // make bucket and table for uid
    spark.sql("DROP TABLE IF EXISTS UsersTbl")
    usersDF.orderBy(asc("uid"))
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      // eual to number of cores I have on my laptop
      .bucketBy(8, "uid")
      .saveAsTable("UsersTbl")

    // make bucket and table for users_id
    spark.sql("DROP TABLE IF EXISTS OrdersTbl")
    ordersDF.orderBy(asc("users_id"))
      .write.format("parquet")
      .bucketBy(8, "users_id")
      .mode(SaveMode.Overwrite)
      .saveAsTable("OrdersTbl")

    // cache tables in memory so that we can see the
    // difference in size in the storage UI
    spark.sql("CACHE TABLE UsersTbl")
    spark.sql("CACHE TABLE OrdersTbl")
    spark.sql("SELECT * from OrdersTbl LIMIT 20")

    // read data back in
    val usersBucketDF = spark.table("UsersTbl")
    val ordersBucketDF = spark.table("OrdersTbl")

    // Now do the join on the bucketed DataFrames
    val joinUsersOrdersBucketDF = ordersBucketDF
      .join(usersBucketDF, $"users_id" === $"uid")

    println("Joined Dataframe made after bucketing!\n")
    joinUsersOrdersBucketDF.show(false)

    // we can see that now we have skipped the Exchange!
    joinUsersOrdersBucketDF.explain()

    // uncomment to view the SparkUI otherwise the program terminates and shutdowsn the UI
    Thread.sleep(1000 * 6 * 1)
  }

}
