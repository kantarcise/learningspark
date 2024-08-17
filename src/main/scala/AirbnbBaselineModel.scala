package learningSpark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._

/**
 * When we evaluated the Linear Regression model with
 * one-hot encoding, we got an RMSE of 220.6.
 *
 * Is this good?
 *
 * Here is a baseline to answer that question.
 */
object AirbnbBaselineModel {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Airbnb ML Pipeline - Baseline Model")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Define the data path as a val
    val airbnbFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-airbnb-clean.parquet"
    }

    // Read the data from the specified path
    val airbnbDF = spark
      .read
      .parquet(airbnbFilePath)

    // Make a train-test split
    val Array(trainDF, testDF) = airbnbDF
      .randomSplit(Array(.8, .2), seed=42)

    // Calculate the average price from the training data
    val avgPrice = trainDF
      .select(avg("price")).first().getDouble(0)

    // Add a column to the test data with the average price prediction
    // If this were a classification problem, we might want
    // to predict the most prevalent class as our baseline model.
    val predDF = testDF
      .withColumn("avgPrediction", lit(avgPrice))

    // Initialize a RegressionEvaluator to evaluate the RMSE for the average price prediction
    val regressionMeanEvaluator = new RegressionEvaluator()
      .setPredictionCol("avgPrediction")
      .setLabelCol("price")
      .setMetricName("rmse")

    // Calculate the RMSE for the average price prediction
    val rmse = regressionMeanEvaluator
      .evaluate(predDF)

    println(f"\nThe RMSE for predicting the average price is: $rmse%1.2f")
  }
}
