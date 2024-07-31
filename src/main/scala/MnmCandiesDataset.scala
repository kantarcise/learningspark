package learningSpark

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Let's use Dataset API instead!
 *
 * Meaning, let's use typed transformations!
 *
 * https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html
 */
object MnmCandiesDataset {

  // Define case class for aggregation result
  case class StateColorCount(State: String, Color: String, Total: Long)

  // Define an aggregator for counting
  object CountAggregator extends Aggregator[Mnm, Long, Long] {
    def zero: Long = 0L
    def reduce(b: Long, a: Mnm): Long = b + 1
    def merge(b1: Long, b2: Long): Long = b1 + b2
    def finish(reduction: Long): Long = reduction
    def bufferEncoder: Encoder[Long] = Encoders.scalaLong
    def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  def main(args: Array[String]): Unit = {

    // define our SparkSession
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MnMCountDataset")
      .getOrCreate()

    // verbose = False
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Define the data path as a val
    val mnmFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/mnm_dataset.csv"
    }

    // Read the data into a Dataset
    val mnmDS: Dataset[Mnm] = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFilePath)
      .as[Mnm]

    // Aggregate counts of all colors and group by State
    // and Color using the CountAggregator
    val countMnMDS = mnmDS
      .groupByKey(mnm => (mnm.State, mnm.Color))
      .agg(CountAggregator.toColumn.name("Total"))
      .map { case ((state, color), total) => StateColorCount(state, color, total) }
      .orderBy(desc("Total"))

    println("\nMnMs grouped by states and colors, counted and ordered\n")
    countMnMDS.show()

    // Filter for California MnMs
    val californiaMnMDS = mnmDS
      .filter(_.State == "CA")
      .groupByKey(mnm => (mnm.State, mnm.Color))
      .agg(CountAggregator.toColumn.name("Total"))
      .map { case ((state, color), total) => StateColorCount(state, color, total) }
      .orderBy(desc("Total"))

    println("Same thing, only for State of California \n")
    californiaMnMDS.show()

    spark.stop()
  }
}
