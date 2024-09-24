package learningSpark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Let's make 2 streaming jobs with different throughput
 * and write into the same DeltaLake table!
 *
 * When there is a single stream, we can write
 * into an empty directory without a problem.
 *
 * However, when there are multiple streams,
 * written into same table, we have to make the
 * table ahead of streaming queries.
 */
object LoansStreamingToDeltaLake {

  // let's scope the case class we'll use
  case class LoanStatus(loan_id: Long,
                        funded_amnt: Int,
                        paid_amnt: Double,
                        addr_state: String)

  // Import global execution context for Futures
  import scala.concurrent.ExecutionContext.Implicits.global

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession()

    spark.sparkContext.setLogLevel("ERROR")

    // Initialize 2 MemoryStreams
    val (loanMemoryStreamFirst, loanMemoryStreamSecond) =
      initializeMemoryStreams(spark)

    // Add sample data every second - one by one!
    val addDataFutureFirst = addDataPeriodicallyToMemoryStream(
      loanMemoryStreamFirst, 1.seconds)

    // every 3 seconds
    val addDataFutureSecond = addDataPeriodicallyToMemoryStream(
      loanMemoryStreamSecond, 3.seconds)

    val loansStreamDSFirst = loanMemoryStreamFirst
      .toDS()

    val loanStreamDSSecond = loanMemoryStreamSecond
      .toDS()

    // Configure Delta Lake path
    val deltaPath = "/tmp/loans_delta_stream"

    // Create Delta table if it doesn't exist
    createDeltaTableIfNotExists(spark, deltaPath)

    // let's make checkpoint directories
    // Each stream should have its own checkpoint directory.
    val checkpointDirFirst = "/tmp/loanCheckpointFirst"
    val checkpointDirSecond = "/tmp/loanCheckpointSecond"

    val query = startStreamingQuery(loansStreamDSFirst,
      deltaPath, checkpointDirFirst, "Loans First Dataset into Delta")

    val querySecond = startStreamingQuery(loanStreamDSSecond,
      deltaPath, checkpointDirSecond, "Loans Second Dataset into Delta")

    // wait 25 seconds to finish
    query.awaitTermination(25000)
    querySecond.awaitTermination(25000)

    // let's make sure data is written as we expect

    // Read back the data from Delta Lake
    val deltaTable = readDeltaTable(spark, deltaPath)

    // Show the data in the console
    deltaTable.show(20)

    // Wait for the data adding to finish (it won't, but in a real
    // use case you might want to manage this better)
    Await.result(addDataFutureFirst, Duration.Inf)
    Await.result(addDataFutureSecond, Duration.Inf)

    // Stop streaming queries and Spark session
    query.stop()
    querySecond.stop()
    spark.stop()
  }

  /**
   * Creates a SparkSession with Delta Lake configurations.
   *
   * @return A configured SparkSession.
   */
  def createSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("Load Streaming Data into DeltaLake")
      .master("local[*]")
      // delta lake configurations
      .config("spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  /**
   * Initializes two MemoryStreams for simulating streaming data sources.
   *
   * @param spark The SparkSession.
   * @return A tuple containing the two MemoryStreams.
   */
  def initializeMemoryStreams(spark: SparkSession): (MemoryStream[LoanStatus],
    MemoryStream[LoanStatus]) = {

    import spark.implicits._
    val loanMemoryStreamFirst = new
        MemoryStream[LoanStatus](1, spark.sqlContext)

    // memory stream with id 2
    val loanMemoryStreamSecond = new
        MemoryStream[LoanStatus](2, spark.sqlContext)

    (loanMemoryStreamFirst, loanMemoryStreamSecond)
  }

  /**
   * Creates the Delta table if it does not exist.
   *
   * @param spark The SparkSession.
   * @param deltaPath The path to the Delta table.
   */
  def createDeltaTableIfNotExists(spark: SparkSession, deltaPath: String): Unit = {
    import io.delta.tables.DeltaTable
    if (!DeltaTable.isDeltaTable(spark, deltaPath)) {
      // Make an empty DataFrame with the schema
      val schemaDF = spark
        .createDataFrame(Seq.empty[LoanStatus])
        .toDF()

      // Write the empty DataFrame to Delta Lake
      // to make the table
      schemaDF
        .write
        .format("delta")
        .save(deltaPath)
    }
  }

  /**
   * Starts a streaming query to write data to Delta Lake.
   *
   * @param dataStream The Dataset[LoanStatus] to write.
   * @param deltaPath The path to the Delta table.
   * @param checkpointDir The checkpoint directory for the stream.
   * @param queryName The name of the streaming query.
   * @return The started StreamingQuery.
   */
  def startStreamingQuery(dataStream: Dataset[LoanStatus],
                          deltaPath: String,
                          checkpointDir: String,
                          queryName: String): StreamingQuery = {
    dataStream
      .writeStream
      .queryName(queryName)
      // .outputMode("update")
      .format("delta")
      .option("checkpointLocation", checkpointDir)
      .start(deltaPath)
  }

  /**
   * Reads data from the Delta Lake table.
   *
   * @param spark The SparkSession.
   * @param deltaPath The path to the Delta table.
   * @return A DataFrame containing the data from the Delta table.
   */
  def readDeltaTable(spark: SparkSession, deltaPath: String): DataFrame = {
    spark
      .read
      .format("delta")
      .load(deltaPath)
  }

  /**
   * Adds sample loan data to a given MemoryStream at specified intervals.
   *
   * This function simulates structured streaming by periodically adding
   * predefined loan data to the provided MemoryStream. Each loan status
   * entry includes a loan ID, funded amount, paid amount, and state.
   * The function uses a random number generator to assign different paid amounts
   * for each loan status.
   *
   * @param memoryStream The MemoryStream to which the sample loan data will be added.
   * @param interval The time interval between each data addition to the MemoryStream.
   * @return A Future[Unit] that completes once all the data has been added to the MemoryStream.
   */
  def addDataPeriodicallyToMemoryStream(memoryStream: MemoryStream[LoanStatus],
                                        interval: FiniteDuration): Future[Unit] = Future {

    val r = new scala.util.Random

    // Generate sample data
    val sampleData = generateSampleLoanData(r)

    // add data one by one
    sampleData.foreach { instance =>
      memoryStream.addData(instance)
      Thread.sleep(interval.toMillis)
    }
  }

  /**
   * Generates sample loan data.
   *
   * @param random An instance of scala.util.Random.
   * @return A sequence of LoanStatus instances.
   */
  def generateSampleLoanData(random: scala.util.Random): Seq[LoanStatus] = {
    // 5 people from California
    // all loaned 1000, paid random amounts
    Seq(
      LoanStatus(1, 1000, random.nextInt(1000).toDouble, "CA"),
      LoanStatus(2, 1000, random.nextInt(1000).toDouble, "CA"),
      LoanStatus(3, 1000, random.nextInt(1000).toDouble, "CA"),
      LoanStatus(4, 1000, random.nextInt(1000).toDouble, "CA"),
      LoanStatus(5, 1000, random.nextInt(1000).toDouble, "CA")
    )
  }
}
