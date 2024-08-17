package learningSpark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._
import org.mlflow.tracking._

import scala.util.Random

/**
 * Let's log some models with MlFlow!
 *
 * Logging is done with MlflowClient
 * https://www.mlflow.org/docs/latest/java_api/org/mlflow/tracking/MlflowClient.html
 *
 * TODO : How to make a new run on existing experiment with MlflowClient?
 */
object MlflowSimple {
  def main(args: Array[String]): Unit = {

    // Initialize Spark session
    val spark = SparkSession
      .builder
      .appName("Spark ML Pipeline with MLflow")
      .master("local[*]")
      // .config("spark.jars.packages", "org.mlflow.mlflow-spark")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Define the data path as a val
    val airbnbFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-airbnb-clean.parquet"
    }

    val airbnbDF = spark
      .read
      .parquet(airbnbFilePath)

    // Split the data into training and test sets
    val Array(trainDF, testDF) = airbnbDF
      .randomSplit(Array(0.8, 0.2), seed = 42)

    val (vecAssembler, rf, pipeline) =
      makeAssemblerRegressorAndPipeline(trainDF)

    // Initialize MlflowClient
    // Replace with your actual MLflow Tracking URI
    val trackingUri = "http://localhost:5000"
    val mlflowClient = new MlflowClient(trackingUri)

    // Start an MLflow run
    val random_string = Random.nextString(3)
    val experimentId = mlflowClient.createExperiment(s"random-forest-${random_string}")
    val runInfo = mlflowClient.createRun(experimentId)
    val runId = runInfo.getRunId

    try {
      // Log parameters: Num Trees and Max Depth
      mlflowClient.logParam(runId, "num_trees", rf.getNumTrees.toString)
      mlflowClient.logParam(runId, "max_depth", rf.getMaxDepth.toString)

      // Fit pipeline model
      val pipelineModel = pipeline
        .fit(trainDF)

      // Make predictions
      val predDF = pipelineModel
        .transform(testDF)

      // Evaluate model
      val regressionEvaluator = new RegressionEvaluator()
        .setPredictionCol("prediction")
        .setLabelCol("price")

      val rmse = regressionEvaluator.setMetricName("rmse").evaluate(predDF)
      val r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)

      // Log metrics: RMSE and R2
      mlflowClient.logMetric(runId, "rmse", rmse)
      mlflowClient.logMetric(runId, "r2", r2)

      // Log artifact: Feature Importance Scores
      val rfModel = pipelineModel
        .stages
        .last
        .asInstanceOf[RandomForestRegressionModel]

      val featureImportances = vecAssembler
        .getInputCols
        .zip(rfModel.featureImportances.toArray)

      val featureImportanceDF = featureImportances
        .toSeq
        .toDF("feature", "importance")
        .orderBy(desc("importance"))

      featureImportanceDF
        .repartition(1)
        .write
        .option("header", "true")
        .csv(s"/tmp/feature-importance-$runId.csv")

      mlflowClient.logArtifact(runId,
        new java.io.File(s"/tmp/feature-importance-$runId.csv"))
    } finally {
      mlflowClient.setTerminated(runId)
    }

    // Stop Spark session
    spark.stop()
  }

  /**
   * Just like the pipeline we had in Chapter 10,
   * set up all necessary stages for a pipeline!
   *
   * @param trainDF: Dataframe to be trained on
   * @return
   */
  def makeAssemblerRegressorAndPipeline(trainDF: DataFrame
                                       ): (VectorAssembler, RandomForestRegressor, Pipeline) = {

    // Identify categorical columns
    val categoricalCols = trainDF.schema.fields.collect {
      case field if field.dataType.typeName == "string" => field.name
    }
    val indexOutputCols = categoricalCols.map(_ + "Index")

    // StringIndexer for categorical columns
    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    // Identify numeric columns
    val numericCols = trainDF.schema.fields.collect {
      case field if field.dataType.typeName == "double" && field.name != "price" => field.name
    }
    val assemblerInputs = indexOutputCols ++ numericCols

    // VectorAssembler to create feature vectors
    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    // RandomForestRegressor model
    val rf = new RandomForestRegressor()
      .setLabelCol("price")
      .setMaxBins(40)
      .setMaxDepth(5)
      .setNumTrees(100)
      .setSeed(42)

    // Build pipeline
    val pipeline = new Pipeline()
      .setStages(Array(stringIndexer, vecAssembler, rf))

    (vecAssembler, rf, pipeline)
  }
}