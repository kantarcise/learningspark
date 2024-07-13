package learningSpark

import org.apache.spark.sql.SparkSession
import scala.util.Random

// heavily inspired from the books example
object SortMergeJoin {

  // curried function to benchmark any code or function
  def benchmark(name: String)(f: => Unit) {
    val startTime = System.nanoTime
    f
    val endTime = System.nanoTime
    println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
  }

  // main class setting the configs
  def main (args: Array[String] ) {

    val spark = SparkSession
      .builder
      .appName("SortMergeJoin")
      // this alone is not enough
      .config("spark.sql.join.preferSortMergeJoin", true)
      // Specifying a value of -1 in spark.sql.autoBroadcastJoinThreshold
      // will cause Spark to always resort to a shuffle sort merge join
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      // The default value is 200, for a 12 core machine
      .config("spark.sql.shuffle.partitions", 24)
      .master(master = "local[*]")
      .getOrCreate ()

    import spark.implicits._

    // Verbose = false
    spark.sparkContext.setLogLevel("ERROR")

    var states = scala.collection.mutable.Map[Int, String]()
    var items = scala.collection.mutable.Map[Int, String]()
    // seed
    val rnd = new Random(42)

    // this should be 16
    println(spark.conf.get("spark.sql.shuffle.partitions"))

    // initialize states and items purchased
    // they are MutableMaps with int keys and string values.
    // When making Dataframes, we will choose randomly from them
    states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4", 5-> "SKU-5")

    // make the dataframes
    val usersDF = (0 to 100000)
      // using the index, generate data with map
      .map(id =>
        (id,
          s"user_${id}",
          s"user_${id}@dreamers.com",
          states(rnd.nextInt(5))))
      .toDF("uid", "login", "email", "user_state")

    val ordersDF = (0 to 100000)
      // using the index, generate data with map
      .map(r =>
          (r,
            r,
            rnd.nextInt(100000),
            10 * r* 0.2d,
            states(rnd.nextInt(5)),
            items(rnd.nextInt(5))))
        .toDF("transaction_id", "quantity", "users_id",
          "amount", "state", "items")

    // lets see how they look
    usersDF.show(10)
    ordersDF.show(10)

    // Do a Sort Merge Join!
    val usersOrdersDF = ordersDF.join(usersDF, $"users_id" === $"uid")

    println("Result of the Sort Merge Join\n")
    usersOrdersDF.show(truncate = false)
    usersOrdersDF.cache()
    usersOrdersDF.explain()
    // usersOrdersDF.explain("formated")

    // uncomment to view the SparkUI
    // if you open the UI, IT shows three stages for the
    // entire job: the Exchange and Sort operations happen in the final stage
    Thread.sleep(1000 * 60 * 7)
  }

}
