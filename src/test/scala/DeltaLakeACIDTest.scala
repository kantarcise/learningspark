package learningSpark

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

// move the case class outside of case class
// because otherwise it will complain
case class LoanStatus(loan_id: Long,
                      funded_amnt: Int,
                      paid_amnt: Double,
                      addr_state: String)

class DeltaLakeACIDTest extends AnyFunSuite with Eventually {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("Delta Lake ACID Guarantees Test")
    .master("local[*]")
    .config("spark.sql.extensions",
      "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  import spark.implicits._

  test("Delta Lake ACID guarantees with concurrent writes") {
    spark.sparkContext.setLogLevel("ERROR")

    // Configure Delta Lake path
    val deltaPath = "/tmp/loans_delta_acid_test"

    // Create Delta table if it doesn't exist
    if (!DeltaTable.isDeltaTable(deltaPath)) {
      spark.createDataset(Seq.empty[LoanStatus])
        .write
        .format("delta")
        .save(deltaPath)
    }

    // Initialize MemoryStream for streaming data
    val loanMemoryStream = new
        MemoryStream[LoanStatus](1, spark.sqlContext)

    // Add sample data to MemoryStream
    addDataPeriodicallyToMemoryStream(loanMemoryStream, 1.seconds)

    val loansStreamDS: Dataset[LoanStatus] = loanMemoryStream
      .toDS()

    // Start streaming query to write data to Delta Lake
    val checkpointDir = "/tmp/loanCheckpoint_acid"
    val streamingQuery = loansStreamDS
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpointDir)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(deltaPath)

    // Concurrent batch write to Delta Lake
    val batchWriteFuture = Future {
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

    // Wait for streaming query and batch write to complete
    streamingQuery.awaitTermination(20000)
    Await.result(batchWriteFuture, Duration.Inf)

    // Read back the data from Delta Lake and verify
    val deltaTable = spark.read.format("delta").load(deltaPath)

    // Verify Atomicity and Consistency
    eventually(timeout(Span(30, Seconds)), interval(Span(2, Seconds))) {
      val count = deltaTable.count()
      assert(count == 8, s"Expected 8 rows, but got $count rows")
      deltaTable.show()
    }

    println("Atomicity and Consistency test passed!")

    // Verify Isolation by checking the
    // consistency of the table after concurrent writes
    val allData = deltaTable
      .as[LoanStatus]
      .collect()
      .toSet

    // our expected will be all 5 CA loans
    // and the 3 NY ones!
    val expectedData = (1 to 5)
      .map(i => LoanStatus(i, 1000, allData.find(_.loan_id == i)
        .map(_.paid_amnt).getOrElse(0.0), "CA")).toSet ++
      Set(
        LoanStatus(1001, 2000, 1500.0, "NY"),
        LoanStatus(1002, 2000, 1500.0, "NY"),
        LoanStatus(1003, 2000, 1500.0, "NY")
      )

    // we can write our own message
    assert(allData == expectedData,
      s"Data in the table did not match expected data")
    println("Isolation test passed!")

    // Verify Durability by reading
    // the data again and ensuring it matches
    val deltaTableAfterRead = spark
      .read
      .format("delta")
      .load(deltaPath)

    val allDataAfterRead = deltaTableAfterRead
      .as[LoanStatus]
      .collect()
      .toSet

    assert(allDataAfterRead == expectedData,
      s"Data in the table did not match expected data after second read")
    println("Durability test passed!")
  }

  def addDataPeriodicallyToMemoryStream(memoryStream: MemoryStream[LoanStatus],
                                        interval: FiniteDuration): Future[Unit] = Future {
    val r = new scala.util.Random

    // Sample data
    val sampleData = Seq(
      LoanStatus(1, 1000, r.nextInt(1000).toDouble, "CA"),
      LoanStatus(2, 1000, r.nextInt(1000).toDouble, "CA"),
      LoanStatus(3, 1000, r.nextInt(1000).toDouble, "CA"),
      LoanStatus(4, 1000, r.nextInt(1000).toDouble, "CA"),
      LoanStatus(5, 1000, r.nextInt(1000).toDouble, "CA")
    )

    // Add data to the MemoryStream one by one
    sampleData.foreach { instance =>
      memoryStream.addData(instance)
      Thread.sleep(interval.toMillis)
    }
  }
}
