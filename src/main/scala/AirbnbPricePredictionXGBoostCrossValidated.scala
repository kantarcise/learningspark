package learningSpark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.sql.functions.{log, exp, col}
import ml.dmlc.xgboost4j.scala.spark.{XGBoostRegressor, XGBoostRegressionModel}

/**
 * Let's use Cross Validation to find the best performing model!
 *
 * https://xgboost.readthedocs.io/en/latest/jvm/xgboost4j_spark_tutorial.html
 */
object AirbnbPricePredictionXGBoostCrossValidated {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession
      .builder
      .appName("Spark ML Pipeline with XGBoost, Cross Validated")
      .master("local[*]")
      .getOrCreate()

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
      .withColumn("label", log(col("price")))
      .randomSplit(Array(0.8, 0.2), seed = 42)

    // Identify categorical columns
    val categoricalCols = trainDF
      // this returns name: type.
      .dtypes
      // filter types
      .filter(_._2 == "StringType")
      // use only the column names
      .map(_._1)

    val indexOutputCols = categoricalCols.map(_ + "Index")

    // StringIndexer for categorical columns
    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    // Identify numeric columns
    val numericCols = trainDF
      .dtypes
      // we can use case inside a filter! Pretty cool.
      .filter{ case (field, dataType) =>
        dataType == "DoubleType" &&
          field != "price" &&
          field != "label"}
      .map(_._1)

    val assemblerInputs = indexOutputCols ++ numericCols

    // VectorAssembler to create feature vectors
    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    // Parameters for XGBoost
    val paramMap = Map(
      "n_estimators" -> 100,
      "learning_rate" -> 0.1,
      "max_depth" -> 4,
      "random_state" -> 42,
      "missing" -> 0
    )

    // XGBoostRegressor model
    val xgboost = new XGBoostRegressor(paramMap)
      .setLabelCol("label")
      .setFeaturesCol("features")

    // Build pipeline
    val pipeline = new Pipeline()
      .setStages(Array(stringIndexer, vecAssembler, xgboost))

    // Hyperparameter tuning
    val paramGrid = new ParamGridBuilder()
      .addGrid(xgboost.maxDepth, Array(4, 6, 8))
      .addGrid(xgboost.numRound, Array(10, 30, 60))
      .addGrid(xgboost.eta, Array(0.3, 0.6))
      .build()

    val evaluator = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(4)

    val cvModel = cv.fit(trainDF)

    // Save the best model
    val bestModelPath = "/tmp/best-xgboost-model"
    cvModel
      .bestModel
      .asInstanceOf[PipelineModel]
      .save(bestModelPath)

    // Make predictions
    val logPredDF = cvModel
      .transform(testDF)

    val expXgboostDF = logPredDF
      .withColumn("prediction", exp(col("prediction")))

    expXgboostDF.select("price", "prediction").show()

    // Evaluate model
    val rmse = evaluator.setMetricName("rmse").evaluate(expXgboostDF)
    val r2 = evaluator.setMetricName("r2").evaluate(expXgboostDF)

    println(s"RMSE is $rmse")
    println(s"R2 is $r2")

    // Stop Spark session
    spark.stop()
  }
}
