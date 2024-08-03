package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc}

object MnmCandies {
  def main(args: Array[String]): Unit = {

    // define our SparkSession
    val spark = SparkSession
      .builder()
      // we will use local mode, to be able to
      // run the applications directly from IDE
      // To learn more:
      // https://spark.apache.org/docs/latest/submitting-applications.html
      .master("local[*]")
      .appName("MnMCount")
      .getOrCreate()

    // verbose = False
    spark.sparkContext.setLogLevel("WARN")

    // now you can use $
    import spark.implicits._

    // for a data source, you can either get the path from user
    // or code it yourself

    /*
    // get file path as argument
    if (args.length < 1) {
      println("Usage: MnMCount <mnm_file_dataset>")
      sys.exit(1)
    }
    // Get the M&M data set filename
    val mnmFile = args(0)
    */

    // Define the data path as a val
    val mnmFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/mnm_dataset.csv"
    }

    // Let spark handle the schema
    // Our csv has a header!
    val mnmDF = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFilePath)

    // aggregate counts of all colors and groupBy() State and Color
    // orderBy() in descending order

    val countMnMDF = mnmDF
      // select 3 cols - it is still a dataframe
      .select($"State", $"Color", $"Count")
      // make groups to use aggregations
      // if you use more columns here
      // you will get more groups!
      .groupBy($"State", $"Color")
      // min max average sum
      .agg(count($"Count").alias("Total"))
      // .orderBy(desc("Total"))
      .orderBy($"Total".desc)

    println("\nMnMs grouped by states and colors, counted and ordered\n")
    countMnMDF.show()

    // california MnM s?
    val californiaMnMDF = mnmDF
      .select("State", "Color", "Count")
      // a simple condition, (alias for filter)
      // .filter(col("State") === "CA")
      // without implicits
      .where(col("State") === "CA")
      // with implicits
      // .where($"State" === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    println("Same thing, only for State of California \n")
    californiaMnMDF.show()

    spark.stop()

  }
}