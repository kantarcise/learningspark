package learningSpark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._
import org.mlflow.tracking._

import java.nio.file.Paths

/**
 * Logging with Context,
 * https://www.mlflow.org/docs/latest/java_api/org/mlflow/tracking/MlflowContext.html
 *
 * In this example we can have multiple runs on the same experiment.
 * Please run it with spark-submit, as a jar.
 *
 * Here is another useful resource:
 * https://docs.databricks.com/en/mlflow/quick-start-java-scala.html
 */
object MlflowWithContext {
  def main(args: Array[String]): Unit = {

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
    val mlflowContext = new MlflowContext(trackingUri)
    val experimentName = "random-forest-experiment"

    val client = mlflowContext.getClient
    val experimentOpt = client
      .getExperimentByName(experimentName)
    if (!experimentOpt.isPresent) {
      client.createExperiment(experimentName)
    }
    mlflowContext.setExperimentName(experimentName)

    val run = mlflowContext.startRun()
    val runId = run.getId

    try {
      // we can set tags
      // We are free to set any tag we want
      // and we can see it in UI!
      run.setTag("author", "Sezai")
      run.setTag("dataset", "Airbnb SF")
      run.setTag("algorithm", "RandomForest")

      // Log parameters: Num Trees, Max Depth and more
      run.logParam("numTrees", rf.getNumTrees.toString)
      run.logParam("maxDepth", rf.getMaxDepth.toString)
      run.logParam("minInfoGain", rf.getMinInfoGain.toString)
      run.logParam("maxMemoryInMB", rf.getMaxMemoryInMB.toString)
      run.logParam("subsamplingRate", rf.getSubsamplingRate.toString)
      run.logParam("impurity", rf.getImpurity)

      //  System Information
      run.logParam("platform", System.getProperty("os.name"))
      run.logParam("java_version", System.getProperty("java.version"))
      run.logParam("scala_version", scala.util.Properties.versionString)
      run.logParam("spark_version", spark.version)

      // Fit pipeline model
      val pipelineModel = pipeline
        .fit(trainDF)

      // Log model
      val modelPath = s"/tmp/pipeline_model_$runId"
      pipelineModel.write.overwrite().save(modelPath)
      run.logArtifacts(Paths.get(modelPath), "model")

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
      run.logMetric("rmse", rmse)
      run.logMetric("r2", r2)

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

      // save the csv as artifact!
      run.logArtifact(Paths.get(s"/tmp/feature-importance-$runId.csv"))

    } finally {
      run.endRun()
    }

    // Stop Spark session
    spark.stop()
  }

  /**
   * Just like the pipeline we had in Chapter 10,
   * set up all necessary stages for a pipeline!
   *
   * Sets up all necessary stages for the ML pipeline:
   * - StringIndexer for categorical features
   * - VectorAssembler for feature vectors
   * - RandomForestRegressor for regression modeling
   *
   * @param trainDF: Dataframe to be trained on
   * @return A tuple containing VectorAssembler,
   *         RandomForestRegressor, and Pipeline
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