package learningSpark

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.DecisionTreeRegressor

/**
 * Demonstrates using a Decision Tree for
 * predicting Airbnb prices.
 */
object AirbnbPricePredictDecisionTree {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Airbnb ML Pipeline - Decision Tree")
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

    val (testDF, trainDF) = trainTestSplit(airbnbDF)

    val pipelineModel = buildAndTrainModel(trainDF)
    visualizeDecisionTree(spark, pipelineModel)

    val predDF = applyModel(pipelineModel, testDF)
    evaluateModel(predDF)

    spark.stop()
  }

  /**
   * Generate the train-test split from the original DataFrame.
   *
   * @param df Input DataFrame.
   * @return A tuple containing the testing and training DataFrames.
   */
  def trainTestSplit(df: DataFrame
                    ): (DataFrame, DataFrame) = {
    val Array(trainDF, testDF) = df.randomSplit(Array(.8, .2), seed=42)
    println(
      f"""\nThere are ${trainDF.count} rows in the training set,
         |and ${testDF.count} in the test set.\n""".stripMargin)
    (testDF, trainDF)
  }

  /**
   * Build and train the Decision Tree model.
   *
   * Tree-based methods can naturally handle
   * categorical variables!
   *
   * @param trainDF Training DataFrame.
   * @return PipelineModel
   */
  def buildAndTrainModel(trainDF: DataFrame): PipelineModel = {
    val categoricalCols = trainDF
      .dtypes
      .filter(_._2 == "StringType")
      .map(_._1)

    // println(categoricalCols.mkString("Array(", ", ", ")"))

    val indexOutputCols = categoricalCols
      .map(_ + "Index")

    // println(indexOutputCols.mkString("Array(", ", ", ")"))

    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    // Let's use the VectorAssembler to combine
    // all of our categorical and numeric inputs
    // https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/feature/VectorAssembler.html

    val numericCols = trainDF
      .dtypes
      // Filter for just numeric columns
      // (and exclude price, our label)
      .filter { case (field, dataType) =>
        dataType == "DoubleType" && field != "price"
      }
      .map(_._1)

    // Combine output of StringIndexer defined
    // above and numeric columns
    val assemblerInputs = indexOutputCols ++ numericCols
    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    // Now let's build a DecisionTreeRegressor
    val dt = new DecisionTreeRegressor()
      .setLabelCol("price")
      // default is 32, we need something bigger than 36
      // because we have we have 36 distinct values
      // While we could increase maxBins to 64
      //  to more accurately represent our continuous
      //  features, that would double the number
      //  of possible splits for continuous variables, greatly
      //  increasing our computation time.
      .setMaxBins(40)

    val stages = Array(stringIndexer, vecAssembler, dt)
    val pipeline = new Pipeline()
      .setStages(stages)

    // perform fit
    pipeline.fit(trainDF)
  }

  /**
   * Visualize the trained Decision Tree model.
   *
   * @param pipelineModel Trained PipelineModel.
   */
  def visualizeDecisionTree(spark: SparkSession,
                            pipelineModel: PipelineModel): Unit = {
    // now we can Visualize the Decision Tree
    val dtModel = pipelineModel.stages.last
      .asInstanceOf[org.apache.spark.ml.regression.DecisionTreeRegressionModel]

    // feature importance scores
    println("Full description of the model:\n")
    println(dtModel.toDebugString)

    println("Feature importance scores:\n")
    println(dtModel.featureImportances)

    // is there a better way to interpret feature importance?
    val featureImp = pipelineModel
      .stages(1)
      .asInstanceOf[VectorAssembler]
      .getInputCols
      .zip(dtModel.featureImportances.toArray)

    val columns = Array("feature", "Importance")

    // val featureImpDF = pipelineModel
    //   .transform(featureImp)
    //   .toDF(columns: _*)

    val featureImpDF = spark
      .createDataFrame(featureImp)
      .toDF(columns: _*)

    featureImpDF
      .orderBy(col("Importance").desc)
      .show()
  }

  /**
   * Apply the trained model to the test DataFrame.
   *
   * Pitfall:
   *   What if we get a massive Airbnb rental?
   *   It was 20 bedrooms and 20 bathrooms. What will a decision tree predict?
   *   It turns out decision trees cannot predict any values
   *   larger than they were trained on. The max value in our
   *   training set was $10,000, so we can't predict any values larger than that.
   *
   * @param pipelineModel Trained PipelineModel.
   * @param testDF Testing DataFrame.
   * @return DataFrame with predictions.
   */
  def applyModel(pipelineModel: PipelineModel, testDF: DataFrame): DataFrame = {

    val predDF = pipelineModel
      .transform(testDF)

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
}
