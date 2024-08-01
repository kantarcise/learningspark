package learningSpark

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}

import scala.util.Random
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

/**
 * Let's try out Kafka for a simplified
 * schema for Nest Thermostats.
 *
 * * This is a great example to discover out Rate Source!
 * *
 * * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources
 */
object NestKafkaProducer {

  // Here is an example schema for a Nest thermostat device.
  val nestSchema = StructType(
    Array(
      StructField("thermostat_model_numbers", StringType, nullable = true),
      StructField("device_id", StringType, nullable = true),
      StructField("temp", DoubleType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true)
    )
  )

  // Define case class for Nest thermostat data
  case class NestThermostat(thermostat_model_numbers: String,
                            device_id: String,
                            temp: Double,
                            timestamp: Timestamp)

  // https://en.wikipedia.org/wiki/Nest_Thermostat
  val modelNumbers = Seq("T100577", "T200577", "T3021US", "GA02082-US")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Kafka Producer with Streaming")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Create a streaming DataFrame using the rate source
    val rateDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "100")
      .load()

    // Generate random data with the specified schema
    val randomDataDF: Dataset[NestThermostat] = rateDF.map(row => {
      val index = row.getAs[Long]("value").toInt
      val rand = new Random()
      val timestamp = Timestamp.valueOf(LocalDateTime.now(ZoneOffset.UTC).plusSeconds(index))

      val thermostatModelNumber = modelNumbers(rand.nextInt(modelNumbers.length))
      val deviceId = s"device_$index"
      val temp = rand.nextDouble() * 100

      NestThermostat(thermostatModelNumber, deviceId, temp, timestamp)
    })

    println("\nThe schema is:\n")
    randomDataDF
      .printSchema()

    // ---------------------- PREPARE DATA FOR KAFKA --------------------

    // Convert DataFrame to JSON format
    /*

    CAST(device_id AS STRING) AS key:

       This part of the expression casts the device_id column
       to a string and renames it to key. Kafka messages consist of
       a key and a value, where the key is often used to partition
       the data and the value is the actual data.

     to_json(struct(*)) AS value:

       This part converts the entire row (all columns) into a JSON string.
       struct(*) creates a struct (a composite data type) from all
       columns in the DataFrame. to_json then converts this struct
       into a JSON string. The resulting JSON string represents the
       data in a format that can be sent to Kafka.
     */
    val jsonDF = randomDataDF
      .selectExpr("CAST(device_id AS STRING) AS key", "to_json(struct(*)) AS value")

    // Write the streaming DataFrame to Kafka
    val query = jsonDF
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "nest-logs")
      .option("checkpointLocation", "/tmp/checkpoint")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }

}
