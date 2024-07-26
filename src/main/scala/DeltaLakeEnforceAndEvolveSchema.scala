package learningSpark

import org.apache.spark.sql.SparkSession

/**
 * This example will demonstrate Delta Lake's ability to
 * enforce and merge schemas.
 *
 * In order to run this application,  run
 * LoansStaticToDeltaLake first, so that you have an
 * existing DeltaLake table.
 */
object DeltaLakeEnforceAndEvolveSchema {
  def main(args: Array[String]): Unit = {

    // a spark session, configured to use Delta lake
    val spark = SparkSession
      .builder
      .appName("Delta Lake Enforce or Merge")
      .master("local[*]")
      .config("spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val deltaPath = "/tmp/loans_delta_enforce_and_merge"

    // Initial DataFrame with original schema
    val initialLoans = Seq(
      (1L, 1000, 1000.0, "CA"),
      (2L, 2000, 2000.0, "NY"),
      (3L, 3000, 3000.0, "TX")
    ).toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state")

    // Write the initial DataFrame to Delta Lake
    initialLoans
      .write
      .format("delta")
      .mode("overwrite")
      .save(deltaPath)

    val loanUpdates = Seq(
      (1111111L, 1000, 1000.0, "TX", false),
      (2222222L, 2000, 0.0, "CA", true))
      .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")

    /*
    // This code will error
    loanUpdates
      .write
      .format("delta")
      .mode("append")
      .save(deltaPath)

    // Error message:
    // A schema mismatch detected when writing to the Delta table

    */

    // After you run the code that failed, you can use:
    loanUpdates
      .write
      .format("delta")
      .mode("append")
      .option("mergeSchema", true)
      .save(deltaPath)

    // Read back the table to verify the schema evolution
    val deltaTable = spark
      .read
      .format("delta")
      .load(deltaPath)

    // Show the schema and data
    deltaTable.printSchema()
    deltaTable.show()

    spark.stop()

  }
}
