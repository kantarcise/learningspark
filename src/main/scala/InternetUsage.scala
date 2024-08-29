package learningSpark

import org.apache.spark.sql.{SparkSession, Dataset}

import scala.util.Random

/**
 * Let's make some data within the code, and run
 * some queries on it.
 *
 * This time with some constants!
 */
object InternetUsage {

  // We can use some Constants to make our code more readable.
  val APP_NAME = "Internet Usage"
  val MASTER = "local[*]"
  val SEED = 42
  val NUM_USERS = 1000
  val EXCESSIVE_USAGE_THRESHOLD = 900
  val SPECIAL_TREATMENT_THRESHOLD = 750
  val SPECIAL_TREATMENT_COST_HIGH = 0.15
  val SPECIAL_TREATMENT_COST_LOW = 0.50

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .appName(APP_NAME)
      .master(MASTER)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Generate usage data
    val internetUsageDS: Dataset[Usage] = generateUsageData(spark)

    // Display generated dataset
    println("\nMade a new dataset with Usage case class!\n")
    internetUsageDS.show(10)

    // Process and display excessive usage data
    displayExcessUsage(internetUsageDS)

    // Process and display special treatment costs
    displaySpecialTreatmentCosts(internetUsageDS)

    // Compute and display user cost usage dataset
    val computeUserCostUsageDS: Dataset[UsageCost] = computeUserCostUsage(internetUsageDS)

    println("Compute Usage - whole DF \n")
    computeUserCostUsageDS
      .show(5, truncate = false)

    // Sleep to check the Web UI
    // Thread.sleep(1000 * 60 * 1)
    spark.close()
  }

  /**
   * Generate some data with a loop, using the Usage
   * case class!
   * @param spark: the SparkSession
   * @return
   */
  def generateUsageData(spark: SparkSession): Dataset[Usage] = {
    import spark.implicits._
    val r = new Random(SEED)

    val data: IndexedSeq[Usage] = (0 to NUM_USERS).map { i =>
      Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000))
    }
    spark.createDataset(data)
  }

  /**
   * Display the excessive usage users, with a
   * filter and a function.
   * @param internetUsageDS: the Usage Dataset
   */
  def displayExcessUsage(internetUsageDS: Dataset[Usage]): Unit = {
    import internetUsageDS.sparkSession.implicits._

    println("Excess usage Without Lambdas\n")
    internetUsageDS
      .where($"usage" > EXCESSIVE_USAGE_THRESHOLD)
      .orderBy($"usage".desc)
      .show(5, truncate = false)

    println("Excess usage with a method (filterWithUsage)!\n")
    internetUsageDS
      .filter(filterWithUsage(_: Usage))
      .orderBy($"usage".desc)
      .show(5, truncate = false)
  }

  /**
   * Calculate and display some Special Price Datasets, calculated
   * based on the Usage Dataset.
   * @param internetUsageDS: the Usage Dataset that is being worked on.
   */
  def displaySpecialTreatmentCosts(internetUsageDS: Dataset[Usage]): Unit = {
    import internetUsageDS.sparkSession.implicits._

    println("special treatment to most users - with just map\n")
    internetUsageDS
      .map((u: Usage) => SpecialPrices(
        if (u.usage > SPECIAL_TREATMENT_THRESHOLD)
          u.usage * SPECIAL_TREATMENT_COST_HIGH
        else u.usage * SPECIAL_TREATMENT_COST_LOW))
      .show(5, truncate = false)

    println("special treatment to most users - with a method (computeCostUsage)!\n")
    internetUsageDS
      .map((u: Usage) => SpecialPrices(computeCostUsage(u.usage)))
      .show(5, truncate = false)
  }

  /**
   * Using the given Usage Dataset,
   * calculate the UsageCost dataset for it.
   *
   * @param internetUsageDS: the Usage Dataset that is being worked on.
   * @return
   */
  def computeUserCostUsage(internetUsageDS: Dataset[Usage]): Dataset[UsageCost] = {
    import internetUsageDS.sparkSession.implicits._

    internetUsageDS
      .map(u => computeUserCostUsage(u))
      .as[UsageCost]
  }

  /**
   * Simply filter the Usage Dataset.
   * @param u: Usage Dataset
   * @return
   */
  def filterWithUsage(u: Usage): Boolean = {
    u.usage > EXCESSIVE_USAGE_THRESHOLD
  }

  /**
   * Calculate the NewPrice based on usage.
   *
   * @param usage: integer value for usage
   * @return
   */
  def computeCostUsage(usage: Int): Double = {
    if (usage > SPECIAL_TREATMENT_THRESHOLD)
      usage * SPECIAL_TREATMENT_COST_HIGH
    else usage * SPECIAL_TREATMENT_COST_LOW
  }

  /**
   * Calculate the UsageCost, based on a given
   * Usage dataset.
   * @param u: Usage dataset
   * @return
   */
  def computeUserCostUsage(u: Usage): UsageCost = {
    val v = if (u.usage > SPECIAL_TREATMENT_THRESHOLD)
      u.usage * SPECIAL_TREATMENT_COST_HIGH
    else u.usage * SPECIAL_TREATMENT_COST_LOW
    UsageCost(u.uid, u.uname, u.usage, v)
  }
}