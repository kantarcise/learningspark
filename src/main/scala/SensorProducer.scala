package learningSpark


import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import scala.util.Random


/**
 * Make 2 different Streaming Dataframes and send them
 * via Kafka to be joined later!
 *
 * This is another great example to discover out Rate Source!
 *
 * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources
 */
object SensorProducer {

  // our case classes
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

    val spark = SparkSession
      .builder
      .appName("Sensor Producer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Setting up rate sources for SmokeDetector and Camera
    val smokeDetectorRateDF = spark.readStream
      .format("rate")
      // 100 gave error,
      // java.lang.OutOfMemoryError: Java heap space
      .option("rowsPerSecond", 10)
      .load()

    val cameraRateDF = spark.readStream
      .format("rate")
      // 100 gave error,
      // java.lang.OutOfMemoryError: Java heap space
      .option("rowsPerSecond", 10)
      .load()

    // Function to generate random SmokeDetector data
    def generateRandomSmokeDetector(index: Long): SmokeDetector = {
      val rand = new Random()
      val hotelRoom = 1000 + rand.nextInt(100)
      val deviceId = s"smoke_detector_$index"
      val status = if (rand.nextBoolean()) "OK" else "ALERT"
      // randomized time
      val timestamp = Timestamp.valueOf(LocalDateTime.now().plus(rand.nextInt(10), ChronoUnit.SECONDS))

      SmokeDetector(hotelRoom, deviceId, status, timestamp)
    }

    // Function to generate random Camera data
    def generateRandomCamera(index: Long): Camera = {
      val rand = new Random()
      val hotelRoom = 1000 + rand.nextInt(100)
      val deviceId = s"camera_$index"
      val isRecording = rand.nextBoolean()
      val isThermal = rand.nextBoolean()
      // randomized time
      val timestamp = Timestamp.valueOf(LocalDateTime.now().plus(rand.nextInt(20), ChronoUnit.SECONDS))

      Camera(hotelRoom, deviceId, isRecording, isThermal, timestamp)
    }

    // Generating SmokeDetector stream
    val smokeDetectorDS: Dataset[SmokeDetector] = smokeDetectorRateDF
      .map(row => generateRandomSmokeDetector(row.getAs[Long]("value")))(Encoders.product[SmokeDetector])

    // Generating Camera stream
    val cameraDS: Dataset[Camera] = cameraRateDF
      .map(row => generateRandomCamera(row.getAs[Long]("value")))(Encoders.product[Camera])

    // Convert SmokeDetector Dataset to JSON format and write to Kafka
    val smokeDetectorJSONDF = smokeDetectorDS
      .selectExpr("CAST(DeviceId AS STRING) AS key", "to_json(struct(*)) AS value")

    val smokeDetectorQuery = smokeDetectorJSONDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "smoke-detectors")
      .option("checkpointLocation", "/tmp/smoke-detectors-checkpoint")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Convert Camera Dataset to JSON format and write to Kafka
    val cameraJSONDF = cameraDS
      .selectExpr("CAST(DeviceId AS STRING) AS key", "to_json(struct(*)) AS value")

    val cameraQuery = cameraJSONDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "cameras")
      .option("checkpointLocation", "/tmp/cameras-checkpoint")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    smokeDetectorQuery.awaitTermination()
    cameraQuery.awaitTermination()
  }
}
