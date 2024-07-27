package learningSpark

import org.apache.spark.sql._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}

/**
 * Now we can get to building an ML pipeline for our data.
 *
 * This application only uses a single
 * feature for LinearRegression to predict prices.
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

    // let's make the train/test splits
    val (testDF, trainDF) = trainTestSplit(airbnbDF)

    // Let's prepare the Features with Transformers
    val (vecAssembler, featureVectorDF) = prepareFeatures(trainDF)

    // model
    val (lr, model) = buildModelWithEstimators(featureVectorDF)

    val pipelineModel = inferencePipeline(trainDF, vecAssembler, lr)

    applyTestDataToPipeline(testDF, pipelineModel)
  }

  /**
   * Generate the train test split from the original Dataframe.
   *
   * The recommendation from the book is to split the data once, then
   * write it out to it's own train/test folder so we don’t
   * have these reproducibility issues (which arises when there are
   * different number of executors in Spark Cluster).
   *
   * @param df: input dataframe
   * @return (testDF, trainDF): Test and Train Datafranes
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
   * After we have split our data into training and test sets, let’s
   * prepare the data to build a linear regression model predicting
   * price given the number of bedrooms.
   *
   * Linear regression (like many other algorithms in Spark) requires that
   * all the input features are contained within a single vector in your
   * DataFrame.
   *
   * Thus, we need to transform our data.
   */
  def prepareFeatures(trainDF: DataFrame
                     ): (VectorAssembler, DataFrame) = {

    // putting all of our features into a single vector
    // this only has "bedrooms" for now
    val vecAssembler = new VectorAssembler()
      .setInputCols(Array("bedrooms"))
      .setOutputCol("features")

    // calling the transform method!
    val vecTrainDF = vecAssembler
      .transform(trainDF)

    // let's see the effect! we now have a features column!
    vecTrainDF
      .select("bedrooms", "features", "price")
      .show(10)

    // see the schema
    // it will have:
    // |-- features: vector (nullable = true)
    vecTrainDF.printSchema()

    // return the VectorAssembler and the
    // Dataframe that has feature column
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
   * Estimators learn parameters from your data, have
   * an estimator_name.fit() method, and are eagerly evaluated (i.e., kick off Spark jobs),
   * whereas transformers are lazily evaluated.
   *
   * Some other examples of estimators include
   * Imputer, DecisionTreeClassifier, and RandomForestRegressor.
   *
   * @param spark
   */
  def buildModelWithEstimators(dfWithVector: DataFrame
                              ): (LinearRegression, LinearRegressionModel) = {

    // our estimator is LinearRegression
    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("price")

    val lrModel = lr
      .fit(dfWithVector)

    // once the estimator learned the parameters
    // the transformer can apply these parameters to new data points!
    val m = lrModel.coefficients(0)
    val b = lrModel.intercept
    println(
      f"""The formula for the linear regression line is
         | 'price = $m%1.2f*bedrooms + $b%1.2f'\n""".stripMargin)

    // return the estimator and the model
    (lr, lrModel)
  }


  /**
   * If we want to apply our model to our test set, then we need to
   * prepare that data in the same way as the training set
   * (i.e., pass it through the vector assembler).
   *
   * With the Pipeline API: we specify the stages we want our data to
   * pass through, in order, and Spark takes care of the processing.
   *
   * In Spark, Pipelines are estimators, whereas
   * PipelineModels (fitted Pipelines) are transformers.
   *
   * Another advantage of using the Pipeline API is that it
   * determines which stages are estimators/transformers for us, so
   * we don’t have to worry about specifying name.fit() versus
   * name.transform() for each of the stages.
   *
   */
  def inferencePipeline(trainDF: DataFrame,
                        vecAssembler: VectorAssembler,
                        lr: LinearRegression) = {

    val pipeline = new Pipeline()
      .setStages(Array(vecAssembler, lr))

    val pipelineModel = pipeline
      .fit(trainDF)

    pipelineModel
  }

  /**
   * Since pipelineModel is a transformer, it is straightforward
   * to apply it to our test dataset.
   *
   * @param testDF: test Dataframe
   * @param pipelineModel: Pipeline Model for inference
   */
  def applyTestDataToPipeline(testDF: DataFrame,
                              pipelineModel: PipelineModel): Unit = {
    val predDF = pipelineModel
      .transform(testDF)

    predDF
      .select("bedrooms", "features", "price", "prediction")
      .show(10)
  }

}
