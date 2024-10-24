package learningSpark

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}

/**
 * Demonstrates using Random Forests for predicting Airbnb prices.
 *
 * TODO: We can make a Utils object for common logic in our methods.
 */
object AirbnbPricePredictionRandomForests {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Airbnb ML Pipeline - Random Forests")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val airbnbDF = loadDataframeFromFileSystem(spark)

    val (trainDF, testDF) = trainTestSplit(airbnbDF)

    val (stringIndexer, vecAssembler, rf,
      pipelineWithRandomForest, paramGrid, evaluator) = prepareStages(trainDF)

    val crossValidatorModelSlow = buildAndTrainModelSlow(trainDF,
      pipelineWithRandomForest, evaluator, paramGrid)

    val pipelineFast = buildAndTrainModelFast(trainDF,
      stringIndexer, vecAssembler,
      rf, evaluator, paramGrid)

    visualizeModel(crossValidatorModelSlow)
    visualizePipelineModel(pipelineFast)

    saveAndLoadModels(pipelineFast)

    val predDF = applyPipelineModel(pipelineFast, testDF)

    println("Here are some metrics on our predictions!\n")
    evaluateModel(predDF)

    spark.stop()
  }

  /**
   * Load the cleaned data from the
   * parquet file.
   *
   * @param spark: The SparkSession
   * @return df: The Dataframe
   */
  def loadDataframeFromFileSystem(spark: SparkSession): DataFrame = {
    // Define the data path as a val
    val airbnbFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-airbnb-clean.parquet"
    }

    val airbnbDF = spark
      .read
      .parquet(airbnbFilePath)

    airbnbDF
  }

  /**
   * Generate the train-test split from the original DataFrame.
   *
   * @param df Input DataFrame.
   * @return A tuple containing the training and
   *         test DataFrames (trainDF, testDF).
   */
  def trainTestSplit(df: DataFrame
                    ): (DataFrame, DataFrame) = {
    val Array(trainDF, testDF) = df.randomSplit(Array(.8, .2), seed=42)
    println(
      f"""\nThere are ${trainDF.count} rows in the training set,
         |and ${testDF.count} in the test set.\n""".stripMargin)
    (trainDF, testDF)
  }

  /**
   * These stages are common in our models.
   *
   * It can be thought as preprocessing steps.
   *
   * @param trainDF : Dataframe to work on
   * @return
   */
  def prepareStages(trainDF: DataFrame): (StringIndexer,
    VectorAssembler, RandomForestRegressor,
    Pipeline, Array[ParamMap], RegressionEvaluator) = {

    val (categoricalCols, numericCols) =
      getCategoricalAndNumericCols(trainDF)

    val indexOutputCols = categoricalCols
      .map(_ + "Index")

    val stringIndexer = makeStringIndexer(categoricalCols, indexOutputCols)

    val assemblerInputs = indexOutputCols ++ numericCols
    val vecAssembler = makeVectorAssembler(assemblerInputs)

    val rf = makeRandomForestRegressor()

    // make the classic pipeline!
    val pipelineWithRandomForest = new Pipeline()
      .setStages(Array(stringIndexer, vecAssembler, rf))

    // Grid search
    // There are a lot of hyperparameters we could tune, and it
    // would take a long time to manually configure.
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxDepth, Array(2, 4, 6))
      .addGrid(rf.numTrees, Array(10, 100))
      // we can also search for
      // minInfoGain, minInstancesPerNode,
      // featureSubsetStrategy.. etc.
      .build()

    val evaluator = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    (stringIndexer, vecAssembler, rf, pipelineWithRandomForest, paramGrid, evaluator)
  }

  /**
   * We also have the option to choose the whole pipeline as the
   * CrossValidaton Estimator, but this will be slower.
   *
   * @param trainDF: Dataframe to work on
   * @param pipelineWithRandomForest: Pipeline with rf last stage
   * @param evaluator: Evaluator
   * @param paramGrid: for Cross Validation
   * @return
   */
  def buildAndTrainModelSlow(trainDF: DataFrame,
                             pipelineWithRandomForest: Pipeline,
                             evaluator: RegressionEvaluator,
                             paramGrid: Array[ParamMap]): CrossValidatorModel = {
    //  Cross Validation
    // We are also going to use 3-fold cross validation
    // to identify the optimal maxDepth.

    // We pass in the `estimator` (pipeline), `evaluator`, and
    // `estimatorParamMaps` to `CrossValidator` so that it knows:
    //    - Which model to use
    //    - How to evaluate the model
    //    - What hyperparameters to set for the model
    //
    // We can also set the number of folds we want to split
    //    our data into (3), as well as setting a seed so
    //        we all have the same split in the data
    val cv = new CrossValidator()
      .setEstimator(pipelineWithRandomForest)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setSeed(42)
      .setParallelism(4)

    // this will kick off spark jobs
    // we just trained 19 models
    // (6 hyperparameter configurations x 3-fold cross-validation)
    // 1 for the optimal hyperparameter selection.
    val cvModelSlow = cv
      .fit(trainDF)

    cvModelSlow
  }

  /**
   * Using the CrossValidator inside pipeline trick,
   * This increases the speed of training.
   *
   * For more information, check out page 320.
   *
   * @param trainDF: Dataframe to work on
   */
  def buildAndTrainModelFast(trainDF: DataFrame,
                             stringIndexer: StringIndexer,
                             vecAssembler: VectorAssembler,
                             rf: RandomForestRegressor,
                             evaluator: RegressionEvaluator,
                             paramGrid: Array[ParamMap]): PipelineModel = {

    // Should we put the pipeline in cross validator, or
    // the cross validator in the pipeline?
    // https://kb.databricks.com/machine-learning/speed-up-cross-validation
    val cvWithRandomForest = new CrossValidator()
      .setEstimator(rf)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(4)
      .setSeed(42)

    val pipelineWithCrossValidator = new Pipeline()
      .setStages(Array(stringIndexer, vecAssembler, cvWithRandomForest))

    // this will kick off a spark job
    val pipelineModel = pipelineWithCrossValidator
      .fit(trainDF)

    pipelineModel
  }

  /**
   * Visualize the trained model and its parameters.
   *
   * @param cvModel CrossValidatorModel
   */
  def visualizeModel(cvModel: CrossValidatorModel): Unit = {

    // after this print, we can see that
    // The best model from our CrossValidator
    // (the one with the lowest RMSE) had
    // maxDepth=6 and numTrees=100.
    print("\nBest model from Cross Validation \n")

    cvModel
      .getEstimatorParamMaps
      .zip(cvModel.avgMetrics)
      .foreach { case (paramMap, metric) =>
        println(s"Param Map: $paramMap => RMSE: $metric")
      }
  }

  /**
   * Visualize the stages and parameters of a trained PipelineModel.
   *
   * @param pipelineModel Trained PipelineModel.
   */
  def visualizePipelineModel(pipelineModel: PipelineModel): Unit = {

    println("\nPipeline Model Stages and Parameters:\n")

    // Iterate through each stage in the pipeline
    pipelineModel.stages.zipWithIndex.foreach { case (stage, index) =>
      println(s"Stage $index: ${stage.getClass.getSimpleName}")

      // Print the parameters and their values for each stage
      stage.extractParamMap().toSeq.foreach { case paramPair =>
        val param = paramPair.param
        val value = paramPair.value
        println(s"  Param: ${param.parent}.${param.name} => Value: $value")
      }
    }

    // If the last stage is a RandomForestRegressionModel, print feature importance
    pipelineModel.stages.last match {
      case rfModel: org.apache.spark.ml.regression.RandomForestRegressionModel =>
        println("\nFeature Importance:")
        rfModel.featureImportances.toArray.zipWithIndex.foreach {
          case (importance, idx) => println(s"Feature $idx: $importance")
        }
      case _ =>
        println("\nThe last stage is not a RandomForestRegressionModel. " +
          "No feature importance to display.\n")
    }
  }

  /**
   * Apply the trained pipeline model to the test DataFrame.
   *
   * @param pipelineModel Trained PipelineModel.
   * @param testDF Testing DataFrame.
   * @return DataFrame with predictions.
   */
  def applyPipelineModel(pipelineModel: PipelineModel,
                 testDF: DataFrame): DataFrame = {

    val predDF = pipelineModel
      .transform(testDF)

    println("\nHere are some of the predictions: \n")
    predDF
      .select("features", "price", "prediction")
      .orderBy(desc("price"))
      .show(truncate = false)
    predDF
  }

  /**
   * Evaluate the model using RMSE and R2 metrics.
   *
   * @param predDF DataFrame with predictions.
   */
  def evaluateModel(predDF: DataFrame): Unit = {
    val regressionEvaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("price")

    val rmse = regressionEvaluator
      .setMetricName("rmse")
      .evaluate(predDF)

    val r2 = regressionEvaluator
      .setMetricName("r2")
      .evaluate(predDF)

    println(s"RMSE is $rmse")
    println(s"R2 is $r2")
    println("*-"*80)
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
    val pipelinePath = "/tmp/random-forest-pipeline-model"

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

  /**
   * Get categorical and numeric columns from the DataFrame.
   *
   * @param df DataFrame
   * @return Tuple containing arrays of categorical and numeric columns.
   */
  def getCategoricalAndNumericCols(df: DataFrame
                                  ): (Array[String], Array[String]) = {
    // Extract the names of categorical columns from the DataFrame.
    val categoricalCols = df
      .dtypes
      .filter(_._2 == "StringType")
      .map(_._1)

    // Extract the numeric columns too
    val numericCols = df
      .dtypes
      .filter { case (field, dataType) =>
        dataType == "DoubleType" && field != "price"
      }
      .map(_._1)

    (categoricalCols, numericCols)
  }

  /**
   * Make a StringIndexer for categorical columns.
   *
   * @param inputCols Array of input column names.
   * @param outputCols Array of output column names.
   * @return StringIndexer
   */
  def makeStringIndexer(inputCols: Array[String],
                          outputCols: Array[String]): StringIndexer = {
    new StringIndexer()
      .setInputCols(inputCols)
      .setOutputCols(outputCols)
      .setHandleInvalid("skip")
  }

  /**
   * Make a VectorAssembler for feature columns.
   *
   * @param inputCols Array of input column names.
   * @return VectorAssembler
   */
  def makeVectorAssembler(inputCols: Array[String]): VectorAssembler = {
    new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("features")
  }

  /**
   * Make a RandomForestRegressor.
   *
   * @return RandomForestRegressor
   */
  def makeRandomForestRegressor(): RandomForestRegressor = {
    new RandomForestRegressor()
      .setLabelCol("price")
      // default is 32, we need something bigger than 36
      // because we have we have 36 distinct values
      .setMaxBins(40)
      .setSeed(42)
  }
}