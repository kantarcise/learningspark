package learningSpark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * An object to load a static Parquet file
 * into a Delta Lake table.
 */
object LoansStaticToDeltaLake {

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    spark.sparkContext.setLogLevel("WARN")

    // File paths can be parameterized or read from configuration
    val loanRiskFilePath = getLoanRiskFilePath
    val deltaTablePath = "/tmp/loans_delta"

    try {
      val loanData = readParquetData(spark, loanRiskFilePath)
      writeDataToDelta(loanData, deltaTablePath)
      printSchema(loanData)
      createTempView(spark, deltaTablePath, "loans_delta")
      runQueries(spark)
    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Ensure the Spark session is stopped to free resources
      spark.stop()
    }
  }

  /**
   * Creates a SparkSession configured with Delta Lake support.
   *
   * @return A SparkSession object.
   */
  def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("Load Static Data To Delta Lake")
      .master("local[*]")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
      )
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .getOrCreate()
  }

  /**
   * Constructs the file path to the loan risk Parquet file.
   *
   * TIP: this method is accessor-like (it just returns a
   * value and doesn't modify any state), so we should
   * remove the empty parentheses.
   *
   * @return The file path as a String.
   */
  private def getLoanRiskFilePath: String = {
    val projectDir = System.getProperty("user.dir")
    s"$projectDir/data/loan-risks.snappy.parquet"
  }

  /**
   * Reads Parquet data from the specified file path.
   *
   * @param spark    The SparkSession object.
   * @param filePath The path to the Parquet file.
   * @return A DataFrame containing the loaded data.
   */
  private def readParquetData(spark: SparkSession,
                              filePath: String): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(filePath)
  }

  /**
   * Writes a DataFrame to a Delta Lake table at the specified path.
   *
   * @param data        The DataFrame to write.
   * @param deltaPath   The destination path for the Delta table.
   */
  private def writeDataToDelta(data: DataFrame,
                               deltaPath: String): Unit = {
    data
      .write
      .mode("overwrite") // Overwrite mode to prevent errors on rerun
      .format("delta")
      .save(deltaPath)
  }

  /**
   * Prints the schema of the DataFrame.
   *
   * @param data The DataFrame whose schema will be printed.
   */
  def printSchema(data: DataFrame): Unit = {
    data.printSchema()
  }

  /**
   * Creates or replaces a temporary view from the Delta table.
   *
   * @param spark    The SparkSession object.
   * @param deltaPath The path to the Delta table.
   * @param viewName The name of the temporary view.
   */
  private def createTempView(spark: SparkSession,
                             deltaPath: String,
                             viewName: String): Unit = {
    spark
      .read
      .format("delta")
      .load(deltaPath)
      .createOrReplaceTempView(viewName)
  }

  /**
   * Runs sample SQL queries on the temporary view.
   *
   * @param spark The SparkSession object.
   */
  private def runQueries(spark: SparkSession): Unit = {
    // Query to count the number of records
    println("Total record count:")
    spark
      .sql("SELECT COUNT(*) FROM loans_delta")
      .show()

    // Query to display the first 5 records
    println("First 5 records:")
    spark
      .sql("SELECT * FROM loans_delta LIMIT 5")
      .show()
  }
}
