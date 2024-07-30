package learningSpark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions.{log, exp, col}
import ml.dmlc.xgboost4j.scala.spark.{XGBoostRegressor, XGBoostRegressionModel}

/**
 * Let's try out XGBoost too
 *
 * The API Docs:
 * https://xgboost.readthedocs.io/en/release_1.1.0/jvm/scaladocs/xgboost4j-spark/ml/dmlc/xgboost4j/scala/spark/index.html
 *
 * Check Out Databricks for more:
 * https://docs.databricks.com/en/machine-learning/train-model/xgboost-scala.html
 */
object AirbnbPricePredictionXGBoost {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("Spark ML Pipeline with XGBoost")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Define the data path as a val
    val airbnbFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-airbnb-clean.parquet"
    }

    val airbnbDF: DataFrame = spark
      .read
      .parquet(airbnbFilePath)

    // Split the data into training and test sets
    val Array(trainDF, testDF) = airbnbDF
      .withColumn("label", log(col("price"))).randomSplit(Array(0.8, 0.2), seed = 42)

    // Identify categorical columns
    val categoricalCols = trainDF.dtypes.collect {
      case (field, dataType) if dataType == "StringType" => field
    }
    val indexOutputCols = categoricalCols.map(_ + "Index")

    // StringIndexer for categorical columns
    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    // Identify numeric columns
    val numericCols = trainDF.dtypes.collect {
      case (field, dataType) if dataType == "DoubleType" && field != "price" && field != "label" => field
    }
    val assemblerInputs = indexOutputCols ++ numericCols

    // VectorAssembler to create feature vectors
    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    // Parameters for XGBoost
    // with these parameters:
    //
    // RMSE is 242.16837012816012
    // R2 is -0.012101191586648241
    val paramMap = Map(
      "n_estimators" -> 100,
      "learning_rate" -> 0.1,
      "max_depth" -> 4,
      "random_state" -> 42,
      "missing" -> 0
    )

    // another set of parameters to try!
    // with these parameters:

    // RMSE is 203.1644080496388
    // R2 is 0.2876648865457073
    val xgbParam = Map("eta" -> 0.3,
      "max_depth" -> 6,
      "objective" -> "reg:squarederror",
      "num_round" -> 10,
      "num_workers" -> 2,
      "missing" -> 0)

    // XGBoostRegressor model
    val xgboost = new XGBoostRegressor(xgbParam)
      .setLabelCol("label")
      .setFeaturesCol("features")

    // Build pipeline
    val pipeline = new Pipeline()
      .setStages(Array(stringIndexer, vecAssembler, xgboost))

    val pipelineModel: PipelineModel = pipeline
      .fit(trainDF)

    // Make predictions
    val logPredDF = pipelineModel
      .transform(testDF)

    val expXgboostDF = logPredDF
      .withColumn("prediction", exp(col("prediction")))

    expXgboostDF.select("price", "prediction").show()

    // Evaluate model
    val regressionEvaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("price")

    val rmse = regressionEvaluator.setMetricName("rmse").evaluate(expXgboostDF)
    val r2 = regressionEvaluator.setMetricName("r2").evaluate(expXgboostDF)

    println(s"RMSE is $rmse")
    println(s"R2 is $r2")

    // Stop Spark session
    spark.stop()
  }
}
