package learningSpark

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._

// Let's use Dataset API instead!
object MnmCandiesDataset {
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

    // Aggregate counts of all colors and group by State and Color
    val countMnMDS = mnmDS
      .groupBy($"State", $"Color")
      .agg(count($"Count").alias("Total"))
      .orderBy(desc("Total"))

    println("\nMnMs grouped by states and colors, counted and ordered\n")
    countMnMDS.show()

    // Filter for California MnMs
    val californiaMnMDS = mnmDS
      .filter($"State" === "CA")
      .groupBy($"State", $"Color")
      .agg(count($"Count").alias("Total"))
      .orderBy(desc("Total"))

    println("Same thing, only for State of California \n")
    californiaMnMDS.show()

    spark.stop()
  }
}
