package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.concurrent.duration._

/**
 * Example demonstrating writing both static
 * and streaming data into the same Delta Lake table.
 */
object LoansStaticAndStreamingToDeltaLake {

  // Case class for loan status
  case class LoanStatus(loan_id: Long,
                        funded_amnt: Int,
                        paid_amnt: Double,
                        addr_state: String)

  def main(args: Array[String]): Unit = {

    // Initialize Spark session with Delta Lake configurations
    val spark = SparkSession
      .builder
      .appName("Static and Streaming to Delta Lake")
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

    // Set log level to ERROR to reduce verbosity
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Configure Delta Lake path
    val deltaPath = "/tmp/loans_delta_stream_and_static"

    try {
      // Write static Dataset to Delta Lake
      writeStaticData(spark, deltaPath)

      // Initialize a MemoryStream for streaming data
      val loanMemoryStream = MemoryStream[LoanStatus](1, spark.sqlContext)

      // Create a streaming Dataset from the MemoryStream
      val loansStreamDS = loanMemoryStream.toDS()

      // Write streaming Dataset to Delta Lake
      val query = writeStreamingData(loansStreamDS, deltaPath)

      // Add sample data to the MemoryStream at intervals
      addDataPeriodicallyToMemoryStream(loanMemoryStream, 1.seconds)

      // Wait for the streaming query to process all data
      query.processAllAvailable()

      // Read back the data from Delta Lake and
      // show it in the console
      readAndShowDeltaTable(spark, deltaPath)

      // Stop the streaming query
      query.stop()

    } finally {
      // Stop the Spark session to free resources
      spark.stop()
    }
  }

  /**
   * Writes static data to the Delta Lake table.
   *
   * @param spark     Spark session
   * @param deltaPath Delta Lake table path
   */
  def writeStaticData(spark: SparkSession,
                      deltaPath: String): Unit = {
    import spark.implicits._

    // Create a static Dataset with sample data
    val staticDS = spark.createDataset(
      Seq(
        LoanStatus(33, 500, 200, "TX"),
        LoanStatus(34, 500, 300, "TX"),
        LoanStatus(35, 500, 450, "TX")
      )
    )

    // Write the static Dataset to Delta Lake in append mode
    staticDS
      .write
      .format("delta")
      .mode("append")
      .save(deltaPath)
  }

  /**
   * Writes streaming data to the Delta Lake table.
   *
   * @param loansStreamDS Streaming Dataset
   * @param deltaPath     Delta Lake table path
   * @return StreamingQuery
   */
  def writeStreamingData(loansStreamDS: Dataset[LoanStatus],
                         deltaPath: String
                        ): StreamingQuery = {
    // Checkpoint directory for streaming query
    val checkpointDir = "/tmp/loanCheckpointThird"

    // Write the streaming Dataset to Delta Lake
    loansStreamDS
      .writeStream
      .queryName("Loans Streaming into Delta")
      .format("delta")
      .option("checkpointLocation", checkpointDir)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(deltaPath)
  }

  /**
   * Reads data from the Delta Lake table and
   * shows it in the console.
   *
   * @param spark     Spark session
   * @param deltaPath Delta Lake table path
   */
  def readAndShowDeltaTable(spark: SparkSession,
                            deltaPath: String): Unit = {
    // Read data from Delta Lake
    val deltaTable = spark
      .read
      .format("delta")
      .load(deltaPath)

    // Show the data in the console
    deltaTable.show(20, truncate = false)
  }

  /**
   * Adds sample data to the MemoryStream at regular intervals.
   *
   * @param memoryStream MemoryStream to add data to
   * @param interval     Interval between data additions
   */
  def addDataPeriodicallyToMemoryStream(memoryStream: MemoryStream[LoanStatus],
                                        interval: FiniteDuration
                                       ): Unit = {
    val random = new scala.util.Random

    // Sample data with random paid amounts
    val sampleData = Seq(
      LoanStatus(1, 1000, random.nextInt(1000).toDouble, "CA"),
      LoanStatus(2, 1000, random.nextInt(1000).toDouble, "CA"),
      LoanStatus(3, 1000, random.nextInt(1000).toDouble, "CA"),
      LoanStatus(4, 1000, random.nextInt(1000).toDouble, "CA"),
      LoanStatus(5, 1000, random.nextInt(1000).toDouble, "CA")
    )

    // Add data to the MemoryStream one by one with delays
    sampleData.foreach { instance =>
      memoryStream.addData(instance)
      Thread.sleep(interval.toMillis)
    }
  }
}
