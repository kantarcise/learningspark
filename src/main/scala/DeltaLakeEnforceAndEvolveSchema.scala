package learningSpark

import org.apache.spark.sql.SparkSession

/**
 * This example demonstrates Delta Lake's ability to
 * enforce and merge schemas.
 *
 * In order to run this application, you need to have
 * Delta Lake dependencies added to your project.
 */
object DeltaLakeEnforceAndEvolveSchema {
  def main(args: Array[String]): Unit = {

    // Make a SparkSession configured to use Delta Lake
    val spark = SparkSession
      .builder()
      .appName("Delta Lake Enforce or Merge Schema Example")
      .master("local[*]")
      // Enable Delta Lake SQL extensions
      .config("spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension")
      // Use Delta Lake catalog
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._

    // Set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    // Path where the Delta Lake table will be stored
    val deltaPath = "/tmp/loans_delta_enforce_and_merge"

    // =========================
    // Step 1: Create Initial DataFrame
    // =========================

    // Initial DataFrame with the original schema
    val initialLoans = Seq(
      (1L, 1000, 1000.0, "CA"),  // Loan 1
      (2L, 2000, 2000.0, "NY"),  // Loan 2
      (3L, 3000, 3000.0, "TX")   // Loan 3
    ).toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state")

    // Display initial schema
    println("Initial Schema:")
    initialLoans.printSchema()

    // Write the initial DataFrame to Delta Lake in overwrite mode
    initialLoans
      .write
      .format("delta")
      .mode("overwrite")  // Overwrite if the table already exists
      .save(deltaPath)

    // =========================
    // Step 2: Create Updated DataFrame with New Schema
    // =========================

    // New DataFrame with an additional column 'closed'
    val loanUpdates = Seq(
      (1111111L, 1000, 1000.0, "TX", false),  // New Loan 1 with 'closed' status
      (2222222L, 2000, 0.0, "CA", true)       // New Loan 2 with 'closed' status
    ).toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")

    // Display the new schema with the additional column 'closed'
    println("\nSchema after updates (with new 'closed' column):")
    loanUpdates.printSchema()

    // =========================
    // Step 3: Attempt to Append Without Schema Merging
    // =========================

    // Try appending without enabling schema merging to demonstrate schema enforcement
    try {
      loanUpdates
        .write
        .format("delta")
        .mode("append")  // Append to the existing table
        .save(deltaPath) // This will throw an exception due to schema mismatch
    } catch {
      case e: Exception =>
        println("\nExpected exception occurred when appending " +
          "data with new schema without schema merging:")
        println(e.getMessage)
    }

    // =========================
    // Step 4: Append with Schema Merging Enabled
    // =========================

    // Now, append the new DataFrame with schema merging enabled
    loanUpdates
      .write
      .format("delta")
      .mode("append")
      .option("mergeSchema", value = true)  // Enable schema merging
      .save(deltaPath)

    // =========================
    // Step 5: Read Back and Display the Delta Table
    // =========================

    // Read back the Delta table to verify schema evolution
    val deltaTable = spark
      .read
      .format("delta")
      .load(deltaPath)

    // Display the evolved schema and data
    println("\nSchema after merging:")
    deltaTable.printSchema()

    println("Data after merging:")
    deltaTable.show()

    // Stop the SparkSession
    spark.stop()
  }
}