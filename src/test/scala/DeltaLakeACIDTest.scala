package learningSpark

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

// Let's define the case class outside of the class
case class LoanStatus(loan_id: Long,
                      funded_amnt: Int,
                      paid_amnt: Double,
                      addr_state: String)

/**
 * Test suite to verify Delta Lake ACID guarantees with concurrent writes.
 */
class DeltaLakeACIDTest extends AnyFunSuite with Eventually {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("Delta Lake ACID Guarantees Test")
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

  import spark.implicits._

  test("Delta Lake ACID guarantees with concurrent writes") {
    spark.sparkContext.setLogLevel("ERROR")

    // Configure Delta Lake path
    val deltaPath = "/tmp/loans_delta_acid_test"

    // Set up Delta table
    setupDeltaTable(deltaPath)

    // Initialize MemoryStream for streaming data
    val loanMemoryStream = initializeMemoryStream()

    // Add sample data to MemoryStream
    val dataFuture = addDataToMemoryStream(loanMemoryStream, 1.seconds)

    // Start streaming query to write data to Delta Lake
    val streamingQuery = startStreamingQuery(loanMemoryStream, deltaPath)

    // Concurrent batch write to Delta Lake
    val batchWriteFuture = performConcurrentBatchWrite(deltaPath)

    // Wait for streaming query and batch write to complete
    waitForQueries(streamingQuery, batchWriteFuture)

    // Read back the data from Delta Lake and verify ACID properties
    verifyACIDProperties(deltaPath)

    // Stop the streaming query
    streamingQuery.stop()

    // Clean up resources
    spark.stop()
  }

  /**
   * Sets up the Delta table if it doesn't exist.
   *
   * @param deltaPath Path to the Delta table.
   */
  def setupDeltaTable(deltaPath: String): Unit = {
    if (!DeltaTable.isDeltaTable(deltaPath)) {
      spark.createDataset(Seq.empty[LoanStatus])
        .write
        .format("delta")
        .save(deltaPath)
    }
  }

  /**
   * Initializes a MemoryStream for LoanStatus.
   *
   * @return MemoryStream[LoanStatus]
   */
  def initializeMemoryStream(): MemoryStream[LoanStatus] = {
    new MemoryStream[LoanStatus](1, spark.sqlContext)
  }

  /**
   * Adds sample data to the MemoryStream periodically.
   *
   * @param memoryStream The MemoryStream to add data to.
   * @param interval     The interval between data additions.
   * @return Future[Unit]
   */
  def addDataToMemoryStream(memoryStream: MemoryStream[LoanStatus],
                            interval: FiniteDuration): Future[Unit] = Future {
    val random = new scala.util.Random

    // Sample data
    val sampleData = generateSampleData(random)

    // Add data to the MemoryStream one by one
    sampleData.foreach { instance =>
      memoryStream.addData(instance)
      Thread.sleep(interval.toMillis)
    }
  }

  /**
   * Generates sample loan data.
   *
   * @param random An instance of Random.
   * @return Seq[LoanStatus]
   */
  def generateSampleData(random: scala.util.Random): Seq[LoanStatus] = {
    Seq(
      LoanStatus(1, 1000, random.nextInt(1000).toDouble, "CA"),
      LoanStatus(2, 1000, random.nextInt(1000).toDouble, "CA"),
      LoanStatus(3, 1000, random.nextInt(1000).toDouble, "CA"),
      LoanStatus(4, 1000, random.nextInt(1000).toDouble, "CA"),
      LoanStatus(5, 1000, random.nextInt(1000).toDouble, "CA")
    )
  }

  /**
   * Starts a streaming query to write data from MemoryStream to Delta Lake.
   *
   * @param memoryStream The MemoryStream source.
   * @param deltaPath    The Delta Lake table path.
   * @return StreamingQuery
   */
  def startStreamingQuery(memoryStream: MemoryStream[LoanStatus],
                          deltaPath: String): StreamingQuery = {
    val loansStreamDS: Dataset[LoanStatus] = memoryStream.toDS()

    val checkpointDir = "/tmp/loanCheckpoint_acid"
    loansStreamDS
      .writeStream
      .queryName("Loan Stream to Delta Testing ACID")
      .format("delta")
      .option("checkpointLocation", checkpointDir)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(deltaPath)
  }

  /**
   * Performs a concurrent batch write to the Delta Lake table.
   *
   * @param deltaPath The Delta Lake table path.
   * @return Future[Unit]
   */
  def performConcurrentBatchWrite(deltaPath: String): Future[Unit] = Future {
    val batchData = Seq(
      LoanStatus(1001, 2000, 1500.0, "NY"),
      LoanStatus(1002, 2000, 1500.0, "NY"),
      LoanStatus(1003, 2000, 1500.0, "NY")
    ).toDS()

    batchData.write
      .format("delta")
      .mode("append")
      .save(deltaPath)
  }

  /**
   * Waits for the streaming query and batch write to complete.
   *
   * @param streamingQuery    The StreamingQuery.
   * @param batchWriteFuture  Future representing the batch write.
   */
  def waitForQueries(streamingQuery: StreamingQuery,
                     batchWriteFuture: Future[Unit]): Unit = {
    streamingQuery.awaitTermination(20000)
    Await.result(batchWriteFuture, Duration.Inf)
  }

  /**
   * Verifies ACID properties of the Delta Lake table.
   *
   * @param deltaPath The Delta Lake table path.
   */
  def verifyACIDProperties(deltaPath: String): Unit = {
    val deltaTable = spark.read.format("delta").load(deltaPath)

    // Verify Atomicity and Consistency
    eventually(timeout(Span(30, Seconds)), interval(Span(2, Seconds))) {
      val count = deltaTable.count()
      assert(count == 8, s"Expected 8 rows, but got $count rows")
    }
    println("Atomicity and Consistency test passed!")

    // Verify Isolation by checking the consistency of the table after concurrent writes
    val allData = deltaTable.as[LoanStatus].collect().toSet

    // Expected data: all 5 CA loans and the 3 NY loans
    val expectedData = (1 to 5).map { i =>
      LoanStatus(
        i,
        1000,
        allData.find(_.loan_id == i).map(_.paid_amnt).getOrElse(0.0),
        "CA"
      )
    }.toSet ++ Set(
      LoanStatus(1001, 2000, 1500.0, "NY"),
      LoanStatus(1002, 2000, 1500.0, "NY"),
      LoanStatus(1003, 2000, 1500.0, "NY")
    )

    assert(
      allData == expectedData,
      s"Data in the table did not match expected data"
    )
    println("Isolation test passed!")

    // Verify Durability by reading the data again and ensuring it matches
    val deltaTableAfterRead = spark.read.format("delta").load(deltaPath)
    val allDataAfterRead = deltaTableAfterRead.as[LoanStatus].collect().toSet

    assert(
      allDataAfterRead == expectedData,
      s"Data in the table did not match expected data after second read"
    )
    println("Durability test passed!")
  }
}
