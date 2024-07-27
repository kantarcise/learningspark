package learningSpark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.{col, log, exp}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.evaluation.RegressionEvaluator

/**
 * This object demonstrates a machine learning pipeline
 * using Apache Spark ML to predict Airbnb prices.
 *
 * We will discover log based prediction, R Formula and
 * save/load for models!
 */
object AirbnbPricePredictionIntermediate {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Airbnb ML Pipeline - Intermediate")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Define the data path as a val
    val airbnbFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-airbnb-clean.parquet"
    }

    val airbnbDF = spark
      .read
      .parquet(airbnbFilePath)

    // let's make the train/test splits
    val (testDF, trainDF) = trainTestSplit(airbnbDF)

    // Model with one-hot encoding
    val firstModel = modelWithOneHotEncoding(testDF, trainDF)

    // Model predicting the price on the log scale
    val secondModel = betterModelWithLogScale(testDF, trainDF)

    // Save and load the model
    saveAndLoadModels(secondModel)
  }

  /**
   * Generate the train test split from the original Dataframe.
   *
   * The recommendation from the book is to split the data once, then
   * write it out to it's own train/test folder so we don’t
   * have these reproducibility issues (which arises when there are
   * different number of executors in Spark Cluster).
   *
   * @param df Input DataFrame.
   * @return A tuple containing the testing and training DataFrames.
   */
  def trainTestSplit(df : DataFrame
                    ): (DataFrame, DataFrame) = {
    val Array(trainDF, testDF) = df.randomSplit(Array(.8, .2), seed=42)
    println(
      f"""There are ${trainDF.count} rows in the training set,
         |and ${testDF.count} in the test set.\n""".stripMargin)
    (testDF, trainDF)
  }

  /**
   * Demonstrates the usage of the RFormula feature transformer.
   *
   * RFormula allows you to specify a model formula using a concise syntax,
   * similar to R's model formulas, for specifying transformations on the
   * input columns. This method initializes an RFormula to create a feature
   * vector and label column from the input data.
   *
   * The formula "price ~ ." specifies that the target
   * variable is `price` and the feature vector should be
   * composed of all other columns.
   *
   * The method configures the following:
   * - `setFormula("price ~ .")`: Specifies the formula for
   *        transforming the data.
   * - `setFeaturesCol("features")`: Sets the name of the output
   *        column containing the feature vector.
   * - `setLabelCol("price")`: Sets the name of the output column
   *        containing the label (target variable).
   * - `setHandleInvalid("skip")`: Configures how to handle invalid
   *        data (e.g., missing values) by skipping those rows.
   */
  def rFormula(): Unit = {

    val rFormula = new RFormula()
      .setFormula("price ~ .")
      .setFeaturesCol("features")
      .setLabelCol("price")
      .setHandleInvalid("skip")
  }

  /**
   * Evaluate Root Mean Square Error
   *
   * You always have to use RMSE against a baseline.
   *
   * @param predDF DataFrame containing the predictions.
   */
  def evaluateModelRMSE(predDF: DataFrame): Unit = {

    val regressionEvaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("price")
      .setMetricName("rmse")

    val rmse = regressionEvaluator
      .evaluate(predDF)

    println(f"RMSE is $rmse%.1f")
  }

  /**
   * R2 values range from negative infinity to 1.
   *
   * The nice thing about using R2 is that we don’t
   * necessarily need to define a baseline
   * model to compare against.
   *
   * @param predDF DataFrame containing the predictions.
   */
  def evaluateModelR2(predDF: DataFrame): Unit = {

    val r2 =  new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("price")
      .setMetricName("r2").evaluate(predDF)

    println(s"R2 is $r2")
  }

  /**
   * Demonstrates a linear regression model with one-hot
   * encoding for categorical features.
   *
   * @param testDF  Testing DataFrame.
   * @param trainDF Training DataFrame.
   */
  def modelWithOneHotEncoding(testDF: DataFrame,
                              trainDF: DataFrame): PipelineModel = {
    val categoricalCols = trainDF
      .dtypes
      .filter(_._2 == "StringType")
      .map(_._1)

    val indexOutputCols = categoricalCols
      .map(_ + "Index")

    val oheOutputCols = categoricalCols
      .map(_ + "OHE")

    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    val oheEncoder = new OneHotEncoder()
      .setInputCols(indexOutputCols)
      .setOutputCols(oheOutputCols)

    val numericCols = trainDF
      .dtypes
      .filter{ case (field, dataType) =>
      dataType == "DoubleType" && field != "price"}
      .map(_._1)

    val assemblerInputs = oheOutputCols ++ numericCols

    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    val lr = new LinearRegression()
      .setLabelCol("price")
      .setFeaturesCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(stringIndexer, oheEncoder, vecAssembler, lr))
    // Or use RFormula
    // val pipeline = new Pipeline().setStages(Array(rFormula, lr))

    val pipelineModel = pipeline
      .fit(trainDF)

    val predDF = pipelineModel
      .transform(testDF)

    predDF
      .select("features", "price", "prediction")
      .show(5)

    evaluateModelRMSE(predDF)
    evaluateModelR2(predDF)
    println("*-"*80)

    pipelineModel
  }

  /**
   * Builds a linear regression model to predict the
   * price on the log scale.
   *
   * @param testDF  Testing DataFrame.
   * @param trainDF Training DataFrame.
   * @return Trained PipelineModel.
   */
  def betterModelWithLogScale(testDF: DataFrame,
                              trainDF: DataFrame): PipelineModel = {

    val logTrainDF = trainDF.withColumn("log_price", log(col("price")))
    val logTestDF = testDF.withColumn("log_price", log(col("price")))

    val rFormula = new RFormula()
      .setFormula("log_price ~ . - price")
      .setFeaturesCol("features")
      .setLabelCol("log_price")
      .setHandleInvalid("skip")

    val lr = new LinearRegression()
      .setLabelCol("log_price")
      .setPredictionCol("log_pred")

    val pipeline = new Pipeline().setStages(Array(rFormula, lr))
    val pipelineModel = pipeline.fit(logTrainDF)
    val predDF = pipelineModel.transform(logTestDF)

    // In order to interpret our RMSE, we need to convert
    // our predictions back from logarithmic scale.

    val expDF = predDF
      .withColumn("prediction", exp(col("log_pred")))

    evaluateModelRMSE(expDF)
    evaluateModelR2(expDF)

    println("*-"*80)
    pipelineModel
  }

  /**
   * Saves the trained model to disk and loads it back.
   *
   * In the event that our cluster goes down, we don’t have
   * to recompute the model
   *
   * @param model Trained PipelineModel.
   */
  def saveAndLoadModels(model: PipelineModel): Unit = {
    val pipelinePath = "/tmp/lr-pipeline-model"

    model
      .write
      .overwrite()
      .save(pipelinePath)

    // Load the saved model
    val savedPipelineModel = PipelineModel.load(pipelinePath)
    // Demonstrate that the model has been
    // loaded correctly
    println("Model loaded successfully from disk.")
  }

}

