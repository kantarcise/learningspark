package learningSpark

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.util.Comparator

/**
 * We can query previous versioned snapshots of a table
 * with DeltaLake!
 *
 * For more information:
 * https://delta.io/blog/2023-02-01-delta-lake-time-travel/
 */
object DeltaLakeTimeTravel {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Delta Lake Time Travel")
      .master("local[*]")
      .config("spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val nums_delta_path = "/tmp/some_numbers"

    // Perform transformations and save to Delta Lake
    transformData(spark, nums_delta_path)

    // Query different versions of the Delta table
    queryDifferentVersionsDeltaTable(spark, nums_delta_path)

    // Time travel using timestamp
    timeTravelDeltaTable(spark, nums_delta_path)

    // Use DeltaTable API to show history
    val deltaTable = DeltaTable
      .forPath(spark, nums_delta_path)

    println("\n Here is the history of the deltaTable\n")
    deltaTable
      .history
      .select("version", "timestamp", "operation")
      .show(truncate = false)

    // Cleanup after running the code
    cleanupAfterAllProcessing(deltaTable, nums_delta_path)

    // Stop the Spark session
    spark.stop()
  }

  /**
   * We can query previous versioned snapshots of a table
   * by using the DataFrameReader options
   * "versionAsOf" and "timestampAsOf"
   *
   * This one demonstrates versionAsOf.
   *
   * Thanks to DeltaLake!
   * @param spark: SparkSession
   */
  def queryDifferentVersionsDeltaTable(spark: SparkSession,
                                       deltaPath: String): Unit = {
    println("After all the Processing on Dataframe, Final version of DF:\n")
    spark
      .read
      .format("delta")
      .load(deltaPath)
      .show()

    println("First version of DF:\n")
    spark
      .read
      .format("delta")
      .option("versionAsOf", 0)
      .load(deltaPath)
      .show()

    println("Second version of DF:\n")
    spark
      .read
      .format("delta")
      .option("versionAsOf", 1)
      .load(deltaPath)
      .show()

    println("Third version of DF:\n")
    spark
      .read
      .format("delta")
      .option("versionAsOf", 2)
      .load(deltaPath)
      .show()
  }

  /**
   * We can query previous versioned snapshots of a table
   * by using the DataFrameReader options
   * "versionAsOf" and "timestampAsOf"
   *
   * This one demonstrates timestampAsOf.
   *
   * @param spark: SparkSession
   * @param deltaPath: Path for DeltaTable
   */
  def timeTravelDeltaTable(spark: SparkSession, deltaPath: String): Unit = {

    println("Most recent version\n")
    spark
      .read
      .format("delta")
      .load(deltaPath)
      .show()

    println("The version from 5 seconds ago\n")
    spark
      .read
      .format("delta")
      .option("timestampAsOf", LocalDateTime.now().minusSeconds(5).toString)
      .load(deltaPath)
      .show()
  }

  /**
   * Make some transformations on a Dataframe and
   * save after each processing to a DeltaTable
   *
   * This is to demonstrate versioning and
   * TimeTravel in DeltaLake.
   *
   * @param spark: SparkSession
   */
  def transformData(spark: SparkSession, deltaPath: String): Unit = {

    import spark.implicits._

    println("\nMake a simple Dataframe and make some transformations on it!\n")
    val firstDF = spark
      .range(0,3)

    firstDF
      .repartition(1)
      .write
      .format("delta")
      .save(deltaPath)

    // add some more data.
    spark
      .range(8,11)
      .repartition(1)
      .write
      .mode("append")
      .format("delta")
      .save(deltaPath)

    // In Scala, a tuple with a single element
    // should not have a trailing comma.
    val secondDF = Seq(55L, 66L, 77L).toDF("id")

    secondDF
      .repartition(1)
      .write
      .mode("overwrite")
      .format("delta")
      .save(deltaPath)
  }

  /**
   * Clean up the Delta table and delete the directory.
   * @param deltaTable: DeltaTable object
   * @param deltaPath: Path for DeltaTable
   */
  def cleanupAfterAllProcessing(deltaTable: DeltaTable, deltaPath: String): Unit = {
    // Deleting all data in the table
    deltaTable.delete()

    // Deleting the directory containing the Delta Lake table
    val path = Paths.get(deltaPath)
    if (Files.exists(path)) {
      Files.walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(Files.delete)
      println(s"Deleted directory $deltaPath")
    } else {
      println(s"Directory $deltaPath does not exist")
    }
  }
}
