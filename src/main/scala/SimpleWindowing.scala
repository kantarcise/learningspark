package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SimpleWindowing {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Window Function Example")
      .master("local[*]")
      .getOrCreate()

    // $ usage
    import spark.implicits._

    // verbose = False
    spark.sparkContext.setLogLevel("ERROR")

    // Sample sales data
    val salesData = Seq(
      ("East", "Alice", 5000),
      ("West", "Alice", 50000),
      ("East", "Bob", 5000),
      ("East", "John", 3000),
      ("West", "David", 6000),
      ("West", "Eva", 7000),
      ("East", "Alex", 1400000)
    ).toDF("region", "salesperson", "amount")

    println("\nsalesData DF made from a Seq\n")
    salesData.show()

    // Calculate total sales per salesperson per region
    val totalSales = salesData
      .groupBy("region", "salesperson")
      .agg(sum("amount").as("total_sales"))

    println("groupBy region, salesperson\n")
    totalSales.show()

    // Define window specification
    val windowSpec = Window
      .partitionBy("region")
      .orderBy(desc("total_sales"))

    // Apply rank window function
    val rankedSales = totalSales
      .withColumn("rank", rank().over(windowSpec))

    // Apply dense_rank window function
    val denseRankedSales = totalSales
      .withColumn("rank", dense_rank().over(windowSpec))

    // Show results
    println("Ranked Sales with rank\n")
    rankedSales.show()

    // you will see, dense rank is dense
    // https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html
    println("Ranked Sales with dense_rank\n")
    denseRankedSales.show()

    // here is a more general comparison of window functions

    val randomDF = spark.createDataFrame(Seq(
      ("a", 10), ("a", 20), ("a", 30),
      ("a", 40), ("a", 40), ("a", 40), ("a", 40),
      ("a", 50), ("a", 50), ("a", 60)
    )).toDF("part_col", "order_col")

    val window = Window
      // all the same parts = a
      .partitionBy("part_col")
      // ordered by order_col
      .orderBy("order_col")

    val dfWithRanks = randomDF
      .withColumn("rank", rank().over(window))
      .withColumn("dense_rank", dense_rank().over(window))
      .withColumn("row_number", row_number().over(window))
      .withColumn("count", count("*").over(window))

    dfWithRanks.show()
  }

}
