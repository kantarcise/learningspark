package learningSpark

import org.apache.spark.sql._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}

/**
 * An example of building a simple ML pipeline using
 * Spark MLlib to predict Airbnb prices.
 *
 * This application uses a single feature, 'bedrooms', to
 * train a Linear Regression model to predict prices.
 */
object AirbnbPricePredictionSimple {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Airbnb ML Pipeline - Simple")
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

    // Let's make the train/test splits
    val (trainDF, testDF) = trainTestSplit(airbnbDF)

    // Prepare the features with transformers
    val (vecAssembler, featureVectorDF) = prepareFeatures(trainDF)

    // Build the model with estimators
    val lr = buildModelWithEstimators(featureVectorDF)

    // Make and fit the inference pipeline
    val pipelineModel = inferencePipeline(trainDF, vecAssembler, lr)

    // Apply the test data to the pipeline
    applyTestDataToPipeline(testDF, pipelineModel)
  }

  /**
   * Splits the input DataFrame into training and test DataFrames.
   *
   * It's recommended to split the data once and reuse the
   * same split for reproducibility, especially when working with
   * different numbers of executors in a Spark cluster.
   *
   * @param df Input DataFrame to split.
   * @return (trainDF, testDF): A tuple containing the training and test DataFrames.
   */
  def trainTestSplit(df: DataFrame): (DataFrame, DataFrame) = {
    val Array(trainDF, testDF) = df.randomSplit(Array(0.8, 0.2), seed = 42)
    println(
      f"""There are ${trainDF.count} rows in the training set,
         |and ${testDF.count} in the test set.\n""".stripMargin)
    (trainDF, testDF)
  }

  /**
   * Prepares the training DataFrame for modeling by
   * assembling the feature(s) into a feature vector.
   *
   * Linear regression (like many other algorithms in Spark) requires that all input
   * features are contained within a single vector column.
   *
   * This function uses VectorAssembler to combine the feature columns
   * into a single 'features' column.
   *
   * @param trainDF The training DataFrame.
   * @return A tuple containing the VectorAssembler and the
   *         transformed DataFrame with a 'features' column.
   */
  def prepareFeatures(trainDF: DataFrame): (VectorAssembler, DataFrame) = {

    // Putting all of our features into a single vector (only "bedrooms" for now)
    val vecAssembler = new VectorAssembler()
      .setInputCols(Array("bedrooms"))
      .setOutputCol("features")

    // Transforming the training DataFrame
    val vecTrainDF = vecAssembler.transform(trainDF)

    // Show the effect: now we have a 'features' column
    println("With a VectorAssembler, now we have a features " +
      "column which is a Vector type!\n")
    vecTrainDF
      .select("bedrooms", "features", "price")
      .show(10)

    // Display the schema
    println("You can see it in the schema too!\n")
    vecTrainDF.printSchema()

    // Return the VectorAssembler and the transformed DataFrame
    (vecAssembler, vecTrainDF)
  }

  /**
   * After setting up our vectorAssembler, we have our data
   * prepared and transformed into a format that our linear
   * regression model expects.
   *
   * In Spark, LinearRegression is a type of estimator — it takes
   * in a DataFrame and returns a Model.
   *
   * Initializes a Linear Regression estimator and fits it to the data.
   *
   * The function creates a LinearRegression estimator, fits it to
   * the input DataFrame, and prints the regression coefficients.
   *
   * @param dfWithVector DataFrame containing the 'features' vector
   *                     column and the label column.
   * @return The LinearRegression estimator.
   */
  def buildModelWithEstimators(dfWithVector: DataFrame): LinearRegression = {

    // Initialize the LinearRegression estimator
    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("price")
      // Set regParam to a non-zero value to avoid overfitting & instability
      // if we dont, we will get the warning:
      // regParam is zero, which might cause numerical instability and overfitting.
      .setRegParam(0.001)

    // Fit the model to the data
    val lrModel = lr.fit(dfWithVector)

    // Extract and print the coefficients
    val m = lrModel.coefficients(0)
    val b = lrModel.intercept
    println(
      f"""The formula for the linear regression line is
         |  'price = $m%1.2f * bedrooms + $b%1.2f'\n""".stripMargin)

    // Return the estimator
    lr
  }

  /**
   * If we want to apply our model to our test set, then we need to
   * prepare that data in the same way as the training set
   * (i.e., pass it through the vector assembler).
   *
   * With the Pipeline API: we specify the stages we want our data to
   * pass through, in order, and Spark takes care of the processing.
   *
   * Builds and fits a Pipeline consisting of feature
   * transformation and the Linear Regression model.
   *
   * This function makes a Pipeline with the provided VectorAssembler
   * and LinearRegression estimator, fits it to the training data, and
   * returns the fitted PipelineModel.
   *
   * @param trainDF Training DataFrame.
   * @param vecAssembler VectorAssembler for transforming features.
   * @param lr LinearRegression estimator.
   * @return The fitted PipelineModel.
   */
  def inferencePipeline(trainDF: DataFrame,
                        vecAssembler: VectorAssembler,
                        lr: LinearRegression): PipelineModel = {

    // Create the pipeline with the stages
    val pipeline = new Pipeline()
      .setStages(Array(vecAssembler, lr))

    // Fit the pipeline to the training data
    val pipelineModel = pipeline.fit(trainDF)

    pipelineModel
  }

  /**
   * Applies the trained PipelineModel to the test
   * DataFrame for predictions.
   *
   * This function uses the PipelineModel to transform
   * the test data, producing predictions, and prints the results.
   *
   * @param testDF Test DataFrame.
   * @param pipelineModel The trained PipelineModel for inference.
   */
  def applyTestDataToPipeline(testDF: DataFrame,
                              pipelineModel: PipelineModel): Unit = {

    println("After we train the pipeline on train data, we can use it for inference!\n")

    // Transform the test DataFrame using the pipeline model
    val predDF = pipelineModel.transform(testDF)

    // Show the prediction results
    println("Here are the result predictions from our pipeline model!\n")
    predDF
      .select("bedrooms", "features", "price", "prediction")
      .show(10)
  }
}
