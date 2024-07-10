package learningSpark

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._
import scala.util.Random

object InternetUsage {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Internet Usage")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Generate usage data
    val internetUsageDS = generateUsageData(spark)

    // Display generated dataset
    println("\nMade a new dataset with Usage case class!\n")
    internetUsageDS.show(10)

    // Process and display excessive usage data
    displayExcessUsage(internetUsageDS)

    // Process and display special treatment costs
    displaySpecialTreatmentCosts(internetUsageDS)

    // Compute and display user cost usage dataset
    val computeUserCostUsageDS = computeUserCostUsage(internetUsageDS)

    println("Compute Usage - whole DF \n")
    computeUserCostUsageDS
      .show(5, truncate = false)

    // Sleep to check the Web UI
    // Thread.sleep(1000 * 60 * 1)

    spark.close()
  }

  def generateUsageData(spark: SparkSession): Dataset[Usage] = {
    import spark.implicits._
    val r = new Random(42)
    val data = (0 to 1000).map { i =>
      Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000))
    }
    spark.createDataset(data)
  }

  def displayExcessUsage(internetUsageDS: Dataset[Usage]): Unit = {
    import internetUsageDS.sparkSession.implicits._

    println("Excess usage WITHOUT LAMBDAS\n")
    internetUsageDS
      .where($"usage" > 900)
      .orderBy($"usage".desc)
      .show(5, truncate = false)

    println("Excess usage with a method (filterWithUsage)!\n")
    internetUsageDS
      // What is this ? Info down below
      .filter(filterWithUsage(_))
      .orderBy($"usage".desc)
      .show(5, truncate = false)
    /*
    In Scala, the underscore (_) is a placeholder syntax that
    represents a lambda function.
    When you write filterWithUsage(_), it is shorthand for
    filter(u => filterWithUsage(u)).
    Essentially, the underscore is used to represent each
    element in the dataset being passed to the filterWithUsage function.
     */

  }

  def displaySpecialTreatmentCosts(internetUsageDS: Dataset[Usage]): Unit = {
    import internetUsageDS.sparkSession.implicits._

    println("special treatment to most users - with just map\n")
    // With just map
    internetUsageDS
      .map(u => if (u.usage > 750) u.usage * .15 else u.usage * .50)
      .show(5, truncate = false)

    println("special treatment to most users - with a method (computeCostUsage)!\n")
    // with function
    internetUsageDS
      .map(u => {computeCostUsage(u.usage)})
      .show(5, false)

    // println("special treatment to most users - WITHOUT lambdas\n")
    // This is Dataframe API
    // internetUsageDS
    //  .withColumn("value", when($"usage" > 750, $"usage" * .15).otherwise($"usage" * .50))
    //  .select($"value")
    //  .show(5, truncate = false)
  }

  def computeUserCostUsage(internetUsageDS: Dataset[Usage]): Dataset[UsageCost] = {
    import internetUsageDS.sparkSession.implicits._

    // Use map() on our original Dataset
    internetUsageDS
      .map(u => {computeUserCostUsage(u)})
      .as[UsageCost]

    // or with DataFrame API
    // internetUsageDS
    //   .withColumn("cost",
    //     when($"usage" > 750, $"usage" * .15)
    //      .otherwise($"usage" * .50))
    //   .as[UsageCost]
  }

  // Filtering the usage - only excessive users.
  def filterWithUsage(u: Usage): Boolean = {
    u.usage > 900
  }

  // Define a function to compute the usage
  def computeCostUsage(usage: Int): Double = {
    if (usage > 750) usage * 0.15 else usage * 0.50
  }

  // Compute user cost usage
  def computeUserCostUsage(u: Usage): UsageCost = {
    val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
    UsageCost(u.uid, u.uname, u.usage, v)
  }
}
