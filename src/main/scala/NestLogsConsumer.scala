package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

/**
 * Consume the logs and run an aggregation!
 *
 * Start by running the NestKafkaProducer app!
 *
 * For that, you can get a jar and submit it
 * with spark-submit, while you have a Kafka instance running.
 *
 * Then when you submit this application, you will see
 * the results on console!
 *
 * Inspired from :
 * https://www.databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html
 */
object NestLogsConsumer {

  // Define schema for Nest thermostat data
  val nestSchema = StructType(
    Array(
      StructField("thermostat_model_numbers", StringType, nullable = true),
      StructField("device_id", StringType, nullable = true),
      StructField("temp", DoubleType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true)
    )
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Kafka Consumer with Streaming - Nest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Read from Kafka topic
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "nest-logs")
      .option("startingOffsets", "latest")
      .load()

    // Convert the value column from Kafka to the schema
    val valueDF = kafkaDF.selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", nestSchema).as("data"))
      .select("data.*")

    // Perform a 5-minute windowed average of temperatures
    val windowedAvgDF = valueDF
      .withWatermark("timestamp", "10 minutes")
      .groupBy(
        window($"timestamp", "2 minutes"),
        $"thermostat_model_numbers"
      )
      .agg(avg($"temp").alias("avg_temp"))

    // Write the results to console
    val query = windowedAvgDF
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      // write to console every 10 seconds
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}