package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Let's try to answer some questions about the data!
 *
 * Using Dataframes and DSL's!
 *
 * We have 4 questions.
 */
object IoTDataframeWorkout {

  // our schema
  val deviceIoTDataSchema = StructType(Array(
    StructField("device_id", LongType, nullable = false),
    StructField("device_name", StringType, nullable = false),
    StructField("ip", StringType, nullable = false),
    StructField("cca2", StringType, nullable = false),
    StructField("cca3", StringType, nullable = false),
    StructField("cn", StringType, nullable = false),
    StructField("latitude", DoubleType, nullable = false),
    StructField("longitude", DoubleType, nullable = false),
    StructField("scale", StringType, nullable = false),
    StructField("temp", LongType, nullable = false),
    StructField("humidity", LongType, nullable = false),
    StructField("battery_level", LongType, nullable = false),
    StructField("c02_level", LongType, nullable = false),
    StructField("lcd", StringType, nullable = false),
    StructField("timestamp", LongType, nullable = false)
  ))

  def main(args: Array[String]) : Unit = {

    val spark = SparkSession
      .builder
      .appName("IotDatasetWorkout")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val iotFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/iot_devices.json"
    }

    val iotDF = spark
      .read
      .format("json")
      .schema(deviceIoTDataSchema)
      .load(iotFilePath)

    val datasetSize = iotDF.count()
    println(s"\nDF size is: $datasetSize")
    // 198164

    // 1. Detect failing devices with battery levels below a threshold.

    // Let's see min max average of battery first
    // To see the some basic statistics about battery_level, we can use
    iotDF.
      summary()
      .show(truncate = false)

    // We then discover:
    //  For battery_level -  %25 = 2, min = 0, max = 9

    // Calculate the max battery level
    val maxBatteryLevel = iotDF
      .agg(max("battery_level"))
      .first()
      .getLong(0)

    // Calculate the threshold (20% of max battery level)
    val batteryThreshold = maxBatteryLevel * 0.20

    // Filter the dataset for battery levels below the threshold
    val lowBatteryDs = iotDF
      .filter($"battery_level" < batteryThreshold)

    val lowBatteryDeviceCount = lowBatteryDs.count()
    // how many are there?
    println(s"Number of Low battery devices (under %20): $lowBatteryDeviceCount")
    // 39727

    // 2. Identify offending countries with high levels of CO2 emissions.

    // High level - more than %75
    // Calculate the 25th and 75th percentiles of the battery_level
    // for this we can use .stat.approxQuantile !
    val co2quantiles = iotDF
      .stat
      .approxQuantile("c02_level", Array(0.25, 0.75), 0.0)

    // 25 is 1000, 75 is 1400
    val co2Threshold25th = co2quantiles(0)
    val co2Threshold75th = co2quantiles(1)

    val offendingCountries = iotDF
      // this was a lambda
      //.filter(d => d.c02_level > co2Threshold75th)
      // this is DSL
      .filter($"c02_level" > co2Threshold75th)

    println("Here are offending countries (high level c02- more than %75):\n")
    offendingCountries.show(truncate = false)

    // 3. Compute the min and max values for temperature,
    // battery level, CO2, and humidity.
    val minBatteryLevel = iotDF
      .agg(min("battery_level"))
      .first()
      .getLong(0)

    println(s"Min Battery Level $minBatteryLevel")
    println(s"Max Battery Level $maxBatteryLevel")

    val mincO2 = iotDF.agg(min("c02_level")).first().getLong(0)
    val maxcO2 = iotDF.agg(max("c02_level")).first().getLong(0)

    println(s"Min CO2 Level: $mincO2")
    println(s"Max CO2 Level: $maxcO2\n")

    val minHumidity = iotDF.agg(min("humidity")).first().getLong(0)
    val maxHumidity = iotDF.agg(max("humidity")).first().getLong(0)

    println(s"Min Humidity $minHumidity")
    println(s"Max Humidity $maxHumidity\n")

    // 4. Sort and group by average temperature, CO2, humidity, and country.
    val avgMetricsByCountry = iotDF
      .groupBy("cn")
      .agg(
        avg("temp").alias("avg_temp"),
        avg("c02_level").alias("avg_co2"),
        avg("humidity").alias("avg_humidity")
      )
      .orderBy(
        desc("avg_temp"),
        desc("avg_co2"),
        desc("avg_humidity")
      )

    println("Sort and group by average temperature, CO2, humidity, and country.\n")
    avgMetricsByCountry.show(false)
  }
}