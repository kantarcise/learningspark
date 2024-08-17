package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration

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
  // let's scope the case class we will use
  case class LoanStatus(loan_id: Long,
                        funded_amnt: Int,
                        paid_amnt: Double,
                        addr_state: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Load Streaming Data into DeltaLake")
      .master("local[*]")
      // deltalake configurations
      .config("spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Initialize 2 MemoryStreams
    val loanMemoryStreamFirst = new
        MemoryStream[LoanStatus](1, spark.sqlContext)

    // memory stream with id 2
    val loanMemoryStreamSecond = new
        MemoryStream[LoanStatus](2, spark.sqlContext)

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

    // let's make a checkpoint directories
    // Each stream should have its own checkpoint directory.
    val checkpointDirFirst = "/tmp/loanCheckpointFirst"
    val checkpointDirSecond = "/tmp/loanCheckpointSecond"

    val query = loansStreamDSFirst
      .writeStream
      .queryName("Loans First Dataset into Delta")
      // .outputMode("update")
      .format("delta")
      .option("checkpointLocation", checkpointDirFirst)
      .start(deltaPath)

    val querySecond = loanStreamDSSecond
      .writeStream
      .queryName("Loans Second Dataset into Delta")
      .format("delta")
      .option("checkpointLocation", checkpointDirSecond)
      .start(deltaPath)

    // wait 25 seconds to finish
    query.awaitTermination(25000)
    querySecond.awaitTermination(25000)

    // let's make sure data is written as we expect

    // Read back the data from Delta Lake
    val deltaTable = spark
      .read
      .format("delta")
      .load(deltaPath)

    // Show the data in the console
    deltaTable.show(20)

    // Wait for the data adding to finish (it won't, but in a real
    // use case you might want to manage this better)
    Await.result(addDataFutureFirst, Duration.Inf)
    Await.result(addDataFutureSecond, Duration.Inf)
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
                                        interval: FiniteDuration): Future[Unit] = Future{

    val r = new scala.util.Random

    // 5 people from California
    // all loaned 1000, paid random amounts
    val sampleData = Seq(
      LoanStatus(1, 1000, r.nextInt(1000).toDouble, "CA"),
      LoanStatus(2, 1000, r.nextInt(1000).toDouble, "CA"),
      LoanStatus(3, 1000, r.nextInt(1000).toDouble, "CA"),
      LoanStatus(4, 1000, r.nextInt(1000).toDouble, "CA"),
      LoanStatus(5, 1000, r.nextInt(1000).toDouble, "CA")
    )

    // add data one by one
    sampleData.foreach {
      instance => memoryStream.addData(instance)
      Thread.sleep(interval.toMillis)
    }
  }
}
