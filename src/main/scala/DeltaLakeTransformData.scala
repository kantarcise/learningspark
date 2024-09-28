package learningSpark

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime

/**
 * Delta Lake provides a variety of transformations for
 * us to work with. Let's explore them!
 *
 * Run DeltaLakeEnforceAndEvolveSchema before
 * this application to see the effect!
 */
object DeltaLakeTransformData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Delta Lake Transformations")
      .master("local[*]")
      .config("spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val deltaPath = "/tmp/loans_delta_enforce_and_merge"

    // we can work on a Delta Table
    val deltaTable = DeltaTable
      .forPath(spark, deltaPath)

    performNullUpdateForClosedDeltaTable(spark, deltaPath, deltaTable)

    deleteAllPaidLoansDeltaTable(deltaTable)

    upsertChangeDataLoansDeltaTable(spark, deltaTable)

    deduplicateDataWhileInsertDeltaTable(spark, deltaTable)

    operationHistoryDeltaTable(deltaTable)

    queryingPreviousSnapshotsDeltaTable(spark, deltaPath)

    spark.stop()
  }

  /**
   * In DeltaLakeEnforceAndEvolveSchema we've added a
   * new column in our data, called closed.
   *
   * After this addition, only the new added LoanStatus
   * objects had values, others were made NULL.
   *
   * Let's perform the logic for closed for every
   * instance in our Data.
   * @param spark: the spark session
   * @param deltaPath : path for Delta Table
   * @param deltaTable: The table itself
   */
  def performNullUpdateForClosedDeltaTable(spark: SparkSession,
                                           deltaPath: String,
                                           deltaTable: DeltaTable): Unit = {

    // Display original data
    println("Original Data before NULL update:\n")
    deltaTable.toDF.show()

    /*
    // Perform update transformation
    // this update example is from the book
    deltaTable
      .update(
        col("addr_state") === "OR",
        Map("addr_state" -> lit("WA"))
    )
    */

    // Perform NULL update
    // On the "closed" column we've made in
    // DeltaLakeEnforceAndEvolveSchema
    deltaTable.update(
      col("closed").isNull,
      Map(
        "closed" -> (
          col("funded_amnt") -
            col("paid_amnt") === 0)
          .cast("boolean"))
    )

    // Display data after update
    println("Updated Data after NULL update:\n")
    deltaTable.toDF.show()

    // Get the last operation metrics
    println("Last Operation Metrics:\n")
    spark
      .sql(s"DESCRIBE HISTORY delta.`$deltaPath`")
      .show(false)

    // Verify data by reading the Delta Lake table
    val updatedData = spark
      .read
      .format("delta")
      .load(deltaPath)

    println("Verified Data:\n")
    updatedData.show()
  }

  /**
   * We can delete based on our business logic.
   *
   * Let's see an example where we delete the data on
   * all loans that have been fully paid off.
   *
   * Deletes all loans from the Delta Table where the loan
   * has been fully paid off.
   *
   * @param deltaTable The DeltaTable object representing the loans table
   */
  def deleteAllPaidLoansDeltaTable(deltaTable: DeltaTable): Unit = {

    // Display original data before the delete operation
    println("=== Data Before Delete Operation ===")
    deltaTable.toDF.show()

    // Delete records where 'funded_amnt' equals 'paid_amnt'
    deltaTable.delete(condition = expr("funded_amnt = paid_amnt"))

    // Display data after the delete operation
    println("=== Data After Delete Operation ===")
    deltaTable.toDF.show()
  }

  /**
   * A common use case is change data capture, where we replicate row changes
   * made in an OLTP table to another table for OLAP workloads.
   *
   * Say we have another table of new loan information, some of which
   * are new loans and others of which are updates to existing loans.
   *
   * In addition, let’s say this changes table has
   * the same schema as the loan_delta table.
   *
   * This merging operation is based on MERGE SQL command.
   *
   * For more information:
   * https://docs.delta.io/latest/concurrency-control.html
   *
   * Performs an upsert (merge) operation to update
   * existing loans and insert new loans.
   *
   * @param spark: the spark session
   * @param deltaTable: The table itself
   */
  def upsertChangeDataLoansDeltaTable(spark: SparkSession,
                                      deltaTable: DeltaTable): Unit = {
    // Display original data
    println("Original Data before Upsert Change Data:\n")
    deltaTable.toDF.show()

    import spark.implicits._

    // Create a DataFrame with updates and new loans
    val loanUpdates = Seq(
      // Update existing loan with 'loan_id' 2222222L
      (2222222L, 2500, 500.0, "CA", true),
      // Insert new loans
      (787878L, 3000, 200.0, "AU", false),
      (595959L, 1000, 400.0, "GN", false)
    ).toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")

    // Perform the merge (upsert) operation
    deltaTable
      .alias("t")
      .merge(loanUpdates.alias("s"), "t.loan_id = s.loan_id")
      .whenMatched.updateAll()
      .whenNotMatched.insertAll()
      .execute()

    // Display original data
    println("Updated Data after Upsert Change Data:\n")
    deltaTable.toDF.show()
  }

  /**
   * We can do many complex things while merging:
   *
   * - Delete actions
   * For example, MERGE ... WHEN MATCHED THEN DELETE.
   *
   * - Clause conditions
   * For example, MERGE ... WHEN MATCHED AND <condition> THEN ....
   *
   * - Optional actions
   * All the MATCHED and NOT MATCHED clauses are optional.
   *
   * - Star syntax
   * For example, UPDATE * and INSERT * to update/insert all
   * the columns in the target table with matching columns
   * from the source data set. The equivalent Delta Lake APIs are
   * updateAll() and insertAll(), which we saw in the previous
   * section.
   *
   * Say we wanted to backfill the loan_delta table with
   * historical data on past loans.
   *
   * But some of the historical data may already have
   * been inserted in the table, and we don’t want to update those
   * records because they may contain more up-to-date information.
   *
   * You can deduplicate by the loan_id while inserting
   * by running the following merge operation with only the
   * INSERT action (since the UPDATE action is optional):
   *
   * Inserts historical data into the Delta Table while
   * avoiding duplicates based on 'loan_id'.
   *
   * @param spark: the spark session
   * @param deltaTable: The table itself
   */
  def deduplicateDataWhileInsertDeltaTable(spark: SparkSession,
                                           deltaTable: DeltaTable): Unit = {

    import spark.implicits._

    // Display original data before inserting historical data
    println("=== Data Before Inserting Historical Data ===")
    deltaTable.toDF.show()

    // let's make some historical Updates
    // 2 already inserted, 2 new instances
    val historicalUpdates = Seq(
      (787878L, 3000, 200.0, "AU", false),
      (595959L, 1000, 400.0, "GN", false),
      (333333L, 2000, 500.0, "AU", false),
      (444444L, 500, 400.0, "PL", false))
      .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")

    // Update the table, with recent favored approached
    deltaTable
      .alias("t")
      .merge(source = historicalUpdates.alias("s"),
        condition = "t.loan_id = s.loan_id"
      )
      .whenNotMatched.insertAll()
      .execute()

    // Display data after inserting historical data
    println("=== Data After Inserting Historical Data ===")
    deltaTable.toDF.show()
  }

  /**
   * All of the changes to our Delta Lake table are
   * recorded as commits in the table’s transaction log.
   *
   * As we write into a Delta Lake table or directory, every
   * operation is automatically versioned!
   *
   * There are different ways to examine this history.
   *
   * Displays the operation history of the Delta Table, including
   * versions and operations performed.
   */
  def operationHistoryDeltaTable(deltaTable: DeltaTable): Unit = {

    // the most simple way
    println("Table History in Detail:\n")
    deltaTable.history().show()

    println("Table History with Key Columns:\n")
    // Print some key columns, like
    // operation and operationParameters
    deltaTable
      .history(3)
      .select("version", "timestamp", "operation", "operationParameters")
      .show(false)
  }

  /**
   *  We can query previous versioned snapshots of a table
   *  by using the DataFrameReader options
   *  "versionAsOf" and "timestampAsOf"
   *
   *  For more information:
   *  https://delta.io/blog/2023-02-01-delta-lake-time-travel/
   *
   * Queries previous snapshots of the Delta Table using
   * time travel features.
   */
  def queryingPreviousSnapshotsDeltaTable(spark: SparkSession,
                                          deltaPath: String): Unit = {

    // Query the Delta Table as of a specific timestamp
    println("DeltaTable after a Timestamp:\n")
    // let's see an example of read after a timestamp
    spark
      .read
      .format("delta")
      // timestamp after table made
      // can be minutes, seconds, hours or days
      .option("timestampAsOf", LocalDateTime.now().minusSeconds(5).toString)
      .load(deltaPath)
      .show(10, truncate = false)

    // Query the Delta Table as of a specific version
    println("After a Version Of:\n")
    spark
      .read
      .format("delta")
      // 0 is the first version
      .option("versionAsOf", "2")
      .load(deltaPath)
      .show(10, truncate = false)
  }
}