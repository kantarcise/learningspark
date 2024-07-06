package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IotDatasetWorkout {
  def main(args: Array[String]) : Unit = {

    val spark = SparkSession
      .builder
      .appName("IotDatasetWorkout")
      .master("local[*]")
      .getOrCreate()

    // verbose = False
    spark.sparkContext.setLogLevel("ERROR")

    // Import implicits for encoders
    import spark.implicits._

    // Define the data path as a val
    val iotFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/iot_devices.json"
    }

    val iotDS = spark
      .read
      .format("json")
      .load(iotFilePath)
      .as[DeviceIoTData]

    val datasetSize = iotDS.count()
    println(s"DS size is: $datasetSize")
    // 198164

    // 1. Detect failing devices with battery levels below a threshold.

    // Let's see min max average of battery first

    // To see the some basic statistics about battery_level, we can use
    iotDS.summary().show(truncate = false)

    // We then discover:
    //  For battery_level
    //     %25 = 2, min = 0, max = 9

    // Calculate the max battery level
    val maxBatteryLevel = iotDS
      .agg(max("battery_level"))
      .first()
      .getLong(0)

    // Calculate the threshold (20% of max battery level)
    val batteryThreshold = maxBatteryLevel * 0.20

    // Filter the dataset for battery levels below the threshold
    val lowBatteryDs = iotDS
      // this was a lambda
      //.filter(d => d.battery_level < batteryThreshold)
      // let's write it in dsl
      .filter($"battery_level" < batteryThreshold)

    // Show the filtered dataset
    // lowBatteryDs.show(truncate = false)

    val lowBatteryDeviceCount = lowBatteryDs.count()
    // how many are there?
    println(s"Number of Low battery devices (under %20): $lowBatteryDeviceCount")
    // 39727

    // 2. Identify offending countries with high levels of CO2 emissions.

    // high level - more than %75
    // Calculate the 25th and 75th percentiles of the battery_level
    // for this we can use .stat.approxQuantile !
    val co2quantiles = iotDS
      .stat
      .approxQuantile("c02_level", Array(0.25, 0.75), 0.0)

    val co2Threshold25th = co2quantiles(0)
    val co2Threshold75th = co2quantiles(1)

    val offendingCountries = iotDS
      // this was a lambda
      //.filter(d => d.c02_level > co2Threshold75th)
      // this is DSL
      .filter($"c02_level" > co2Threshold75th)

    // TODO - Here are offending countries
    println("Here are offending countries (high level c02- more than %75):\n")
    offendingCountries.show(truncate = false)

    // 3. Compute the min and max values for temperature,
    // battery level, CO2, and humidity.
    val minBatteryLevel = iotDS
      .agg(min("battery_level"))
      .first()
      .getLong(0)

    println(s"Min Battery Level $minBatteryLevel")

    val mincO2 = iotDS
      .agg(min("c02_level"))
      // .collect()(0)
      // .take(1)(0)
      .first()
      .getLong(0)

    val maxcO2 = iotDS
      .agg(max("c02_level"))
      .first()
      .getLong(0)

    println(s"Min CO2 Level: $mincO2")
    println(s"Max CO2 Level: $maxcO2\n")

    val minHumidity = iotDS
      .agg(min("humidity"))
      .first()
      .getLong(0)

    val maxHumidity = iotDS
      .agg(max("humidity"))
      .first()
      .getLong(0)

    println(s"Min Humidity $minHumidity")
    println(s"Max Humidity $maxHumidity\n")

    // 4. Sort and group by average temperature, CO2, humidity, and country.
    val avgMetricsByCountry = iotDS
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
