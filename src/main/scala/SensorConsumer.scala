package learningSpark

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import java.sql.Timestamp

/**
 * Consume 2 streams and join them with Watermarks!
 *
 * Inspired from :
 * https://www.databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html
 */
object SensorConsumer {

  case class SmokeDetector(HotelRoom: Long,
                           DeviceId: String,
                           Status: String,
                           Timestamp: Timestamp)
  case class Camera(HotelRoom: Long,
                    DeviceId: String,
                    IsRecording: Boolean,
                    IsThermal: Boolean,
                    Timestamp: Timestamp)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Sensor Consumer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Define the schema for SmokeDetector and Camera
    val smokeDetectorSchema = Encoders.product[SmokeDetector].schema
    val cameraSchema = Encoders.product[Camera].schema

    // Kafka options
    val kafkaBootstrapServers = "localhost:9092"
    val smokeDetectorTopic = "smoke-detectors"
    val cameraTopic = "cameras"

    // Read SmokeDetector stream from Kafka
    val smokeDetectorStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", smokeDetectorTopic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", smokeDetectorSchema).as("data"))
      .select("data.*")
      .withWatermark("Timestamp", "1 minute")
      .alias("smokeDetector")

    // Read Camera stream from Kafka
    val cameraStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", cameraTopic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", cameraSchema).as("data"))
      .select("data.*")
      .withWatermark("Timestamp", "1 minute")
      .alias("camera")

    // Join streams on HotelRoom
    // this gave - out of memory error
    // val joinedStream = smokeDetectorStream.join(
    //   cameraStream,
    //   expr("""
    //     smokeDetector.HotelRoom = camera.HotelRoom AND
    //     smokeDetector.Timestamp >= camera.Timestamp - interval 1 minutes AND
    //     smokeDetector.Timestamp <= camera.Timestamp + interval 1 minutes
    //   """)
    // )

    // just simply join with watermarks
    val joinedStream = smokeDetectorStream.join(cameraStream,
      smokeDetectorStream("HotelRoom") === cameraStream("HotelRoom") )

    // Write joined stream to console
    val query = joinedStream.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
