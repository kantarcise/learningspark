package learningSpark

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * Let's use a model that we trained in a
 * Spark streaming application!
 *
 * Run the AirbnbPricePredictionRandomForests before this one!
 * 
 * Because the model will be trained and saved in that Application, 
 * which will be used here.
 */
object AirbnbPricePredictionStreaming {

  def main(args: Array[String]) :Unit = {

    val spark = SparkSession
      .builder
      .appName("Spark Streaming for Price Prediction")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // load saved model
    val pipelinePath = "/tmp/random-forest-pipeline-model"
    val pipelineModel = PipelineModel.load(pipelinePath)

    // Define the data path as a val
    val repartitionedPath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-airbnb-clean-100p.parquet"
    }

    // we have to define schema in streaming applications
    val schema = spark
      .read
      .parquet(repartitionedPath)
      .schema

    val streamingData = spark
      .readStream
      // We can set the schema this way
      .schema(schema)
      .option("maxFilesPerTrigger", 1)
      .parquet(repartitionedPath)

    // generate predictions
    val streamPredictions = pipelineModel
      .transform(streamingData)

    // Write the predictions to the console
    val query: StreamingQuery = streamPredictions
      .writeStream
      .queryName("Stream Predictions to Console")
      .outputMode("append")
      .format("console")
      .start()

    // Wait for the stream to finish
    query.awaitTermination()
  }
}