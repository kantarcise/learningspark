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
      f"""\nThere are ${trainDF.count} rows in the training set,
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
   *
   * The downside of RFormula automatically combining the StringIndexer
   * and OneHotEncoder is that one-hot encoding is not required or
   * recommended for all algorithms.
   *
   * For example, tree-based algorithms can handle categorical
   * variables directly if you just use the StringIndexer for the
   * categorical features. You do not need to onehot encode categorical
   * features for tree-based methods, and it will often make your
   * tree-based models worse.
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
   * CHeck out page 302 for more information.
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
   * CHeck out page 304 for more information.
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
   * @param testDF  Test DataFrame.
   * @param trainDF Training DataFrame.
   * @return The trained PipelineModel.
   */
  def modelWithOneHotEncoding(testDF: DataFrame,
                              trainDF: DataFrame): PipelineModel = {
    // Extract the names of categorical columns
    // from the training DataFrame
    val categoricalCols = trainDF
      .dtypes
      // all the Strings
      .filter(_._2 == "StringType")
      // names only
      .map(_._1)

    // Make output column names for
    // the StringIndexer transformation
    val indexOutputCols = categoricalCols
      .map(_ + "Index")

    // Make output column names for
    // the OneHotEncoder transformation
    val oheOutputCols = categoricalCols
      .map(_ + "OHE")

    // Initialize StringIndexer to convert
    // categorical string columns to indexed numeric columns
    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    // Initialize OneHotEncoder to convert indexed
    // numeric columns to one-hot encoded vectors
    val oheEncoder = new OneHotEncoder()
      .setInputCols(indexOutputCols)
      .setOutputCols(oheOutputCols)

    // Extract the names of numeric columns from the
    // training DataFrame, excluding the label column "price"
    val numericCols = trainDF
      .dtypes
      .filter { case (field, dataType) =>
        dataType == "DoubleType" && field != "price"
      }
      // only names
      .map(_._1)

    // Combine the one-hot encoded columns
    // and numeric columns for the feature vector
    val assemblerInputs = oheOutputCols ++ numericCols

    // Initialize VectorAssembler to combine
    // feature columns into a single feature vector
    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    // Initialize LinearRegression model
    val lr = new LinearRegression()
      .setLabelCol("price")
      .setFeaturesCol("features")
      .setRegParam(0.001)

    // Make a pipeline with these stages:
    //    StringIndexer, OneHotEncoder,
    //    VectorAssembler, and LinearRegression
    val pipeline = new Pipeline()
      .setStages(Array(stringIndexer, oheEncoder, vecAssembler, lr))

    // Fit the pipeline model on the training DataFrame
    val pipelineModel = pipeline.fit(trainDF)

    // Transform the test DataFrame using
    // the trained pipeline model to make predictions
    val predDF = pipelineModel.transform(testDF)

    println("Here are the results on the Test data!")
    println("Features is a sparse vector!\n")
    // Show the first 5 rows of the features, actual price, and predicted price
    predDF
      .select("features", "price", "prediction")
      .show(5)

    println("Here is the schema for it! (double check that it's a vector)\n")
    predDF
      .select("features", "price", "prediction")
      .printSchema()

    // Evaluate the model using RMSE (Root Mean Square Error)
    println("Metrics of the model without log scale prediction.\n")
    evaluateModelRMSE(predDF)

    // Evaluate the model using R2 (Coefficient of Determination)
    evaluateModelR2(predDF)

    // Print a separator for clarity
    println("*-" * 80)

    // Return the trained pipeline model
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

    // Apply logarithm transformation to
    // the "price" column in the test and train DataFrames
    val logTrainDF = trainDF.withColumn("log_price", log(col("price")))
    val logTestDF = testDF.withColumn("log_price", log(col("price")))

    // Define an RFormula to specify the
    // model formula and input/output column names
    val rFormula = new RFormula()
      // Predict log_price using all other features except price
      .setFormula("log_price ~ . - price")
      // Define the name of the features column
      .setFeaturesCol("features")
      // Define the name of the label column
      .setLabelCol("log_price")
      // Skip rows with invalid values
      .setHandleInvalid("skip")

    // Initialize a LinearRegression model to
    // predict the log of the price
    val lr = new LinearRegression()
      .setLabelCol("log_price") // Set the label column to log_price
      .setPredictionCol("log_pred") // Set the prediction column name to log_pred
      .setRegParam(0.001)

    // Make a pipeline with stages:
    //    RFormula and LinearRegression
    val pipeline = new Pipeline()
      .setStages(Array(rFormula, lr))

    // Fit the pipeline model on the training DataFrame
    val pipelineModel = pipeline
      .fit(logTrainDF)

    // Transform the test DataFrame using the
    // trained pipeline model to make predictions
    val predDF = pipelineModel
      .transform(logTestDF)

    // Convert the predictions back
    // from logarithmic scale to original scale
    val expDF = predDF
      .withColumn("prediction", exp(col("log_pred")))

    println("\nMetrics of the model with log scale prediction.")
    println("If our approach is correct, these should be better!\n")

    // Evaluate the model using
    // RMSE (Root Mean Square Error) on the original scale
    evaluateModelRMSE(expDF)

    // Evaluate the model using
    // R2 (Coefficient of Determination) on the original scale
    evaluateModelR2(expDF)

    // Print a separator for clarity
    println("*-" * 80)

    // Return the trained pipeline model
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

    println(s"Model saved to $pipelinePath.")

    // Load the saved model
    val savedPipelineModel = PipelineModel.load(pipelinePath)
    // Demonstrate that the model has been
    // loaded correctly
    println("Model loaded successfully from disk.")
  }

}

