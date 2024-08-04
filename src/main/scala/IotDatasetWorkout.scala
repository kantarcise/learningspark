package learningSpark

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Let's try to answer some questions about the data!
 *
 * But this time, using only typed transformations.
 *
 * We will use some utility methods and an Aggregator!
 *
 * We have 4 questions.
 */
object IotDatasetWorkout {

  // We need some case classes for our reports!
  case class AvgMetrics(tempSum: Double, co2Sum: Double,
                        humiditySum: Double, count: Long)
  case class AvgMetricsByCountry(cn: String, avgTemp: Double,
                                 avgCo2: Double, avgHumidity: Double)
  /**
   * Aggregator to compute the average temperature, CO2 level,
   * and humidity for each country from IoT device data.
   */
  object AvgMetricsAggregator extends Aggregator[DeviceIoTData, AvgMetrics, AvgMetrics] {

    /**
     * Zero value for the aggregator. This represents the initial state
     * of the buffer before any data is processed.
     *
     * @return AvgMetrics with initial sums as 0.0 and count as 0L.
     */
    def zero: AvgMetrics = AvgMetrics(0.0, 0.0, 0.0, 0L)

    /**
     * Method to update the buffer with an input row.
     *
     * @param buffer Current state of the buffer.
     * @param device Current input row.
     * @return Updated buffer with the values from the input row added.
     */
    def reduce(buffer: AvgMetrics, device: DeviceIoTData): AvgMetrics = {
      AvgMetrics(
        buffer.tempSum + device.temp,
        buffer.co2Sum + device.c02_level,
        buffer.humiditySum + device.humidity,
        buffer.count + 1
      )
    }

    /**
     * Method to merge two buffers together.
     *
     * @param b1 First buffer.
     * @param b2 Second buffer.
     * @return Merged buffer with values summed from both input buffers.
     */
    def merge(b1: AvgMetrics, b2: AvgMetrics): AvgMetrics = {
      AvgMetrics(
        b1.tempSum + b2.tempSum,
        b1.co2Sum + b2.co2Sum,
        b1.humiditySum + b2.humiditySum,
        b1.count + b2.count
      )
    }

    /**
     * Method to compute the final output from the reduced buffer.
     *
     * @param reduction Final state of the buffer.
     * @return AvgMetrics with average values calculated.
     */
    def finish(reduction: AvgMetrics): AvgMetrics = {
      AvgMetrics(
        reduction.tempSum / reduction.count,
        reduction.co2Sum / reduction.count,
        reduction.humiditySum / reduction.count,
        reduction.count
      )
    }

    /**
     * Encoder for the intermediate value type (buffer).
     *
     * @return Encoder for AvgMetrics.
     */
    def bufferEncoder: Encoder[AvgMetrics] = Encoders.product[AvgMetrics]

    /**
     * Encoder for the final output value type.
     *
     * @return Encoder for AvgMetrics.
     */
    def outputEncoder: Encoder[AvgMetrics] = Encoders.product[AvgMetrics]
  }

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

    val iotDS = spark
      .read
      .format("json")
      .load(iotFilePath)
      .as[DeviceIoTData]

    val datasetSize = iotDS.count()
    println(s"\nDS size is: $datasetSize")
    // 198164

    // 1. Detect failing devices with battery levels below a threshold.

    // Let's see min max average of battery first.
    // To see the some basic statistics about battery_level, we can use
    iotDS
      .summary()
      .show(truncate = false)

    // We then discover:
    //  For battery_level - %25 = 2, min = 0, max = 9

    // Calculate the max battery level using typed transformations
    val maxBatteryLevel = iotDS
      .map(_.battery_level)
      .reduce((value1, value2) => if (value1 > value2) value1 else value2)

    // Calculate the threshold (20% of max battery level)
    val batteryThreshold = maxBatteryLevel * 0.20

    // Filter the dataset for battery levels below the threshold
    val lowBatteryDs = iotDS
      .filter(d => d.battery_level < batteryThreshold)

    val lowBatteryDeviceCount = lowBatteryDs.count()
    // how many are there?
    println(s"Number of Low battery devices (under %20): $lowBatteryDeviceCount")
    // 39727

    // 2. Identify offending countries with high levels of CO2 emissions.

    // High level - more than %75
    // Calculate the 25th and 75th percentiles of the battery_level
    // approxQuantile is highly optimized but we can replicate it's behaviour!
    val c02Levels = iotDS
      .map(_.c02_level)
      .collect()
      .sorted

    // 25 is 1000, 75 is 1400
    // val co2Threshold25th = getQuantile(c02Levels, 0.25)
    val co2Threshold75th = getQuantile(c02Levels, 0.75)

    val offendingCountries = iotDS
      // this is a lambda
      .filter(d => d.c02_level > co2Threshold75th)
      // this was DSL
      // .filter($"c02_level" > co2Threshold75th)

    println("Here are offending countries (high level c02- more than %75):\n")
    offendingCountries.show(truncate = false)

    // 3. Compute the min and max values for temperature,
    // battery level, CO2, and humidity.

    // to give selector parameter, we are using a lambda function
    // with a fully specified type.
    val (minTemp, maxTemp) = calculateMinMax(iotDS, (data: DeviceIoTData) => data.battery_level)
    val (minBattery, maxBattery) = calculateMinMax(iotDS, (data: DeviceIoTData) => data.battery_level)
    val (minCO2, maxCO2) = calculateMinMax(iotDS, (data: DeviceIoTData) => data.c02_level)
    val (minHumidity, maxHumidity) = calculateMinMax(iotDS, (data: DeviceIoTData) => data.humidity)

    println(s"Min Temperature: $minTemp, Max Temperature: $maxTemp")
    println(s"Min Battery Level: $minBattery, Max Battery Level: $maxBattery")
    println(s"Min CO2 Level: $minCO2, Max CO2 Level: $maxCO2")
    println(s"Min Humidity: $minHumidity, Max Humidity: $maxHumidity")

    // 4. Sort and group by average temperature, CO2, humidity, and country.
    val avgMetricsByCountry = iotDS
      .groupByKey(_.cn)
      .agg(AvgMetricsAggregator.toColumn)
      .map { case (cn, avgMetrics) =>
        AvgMetricsByCountry(cn, avgMetrics.tempSum,
          avgMetrics.co2Sum, avgMetrics.humiditySum)
      }
      .orderBy(
        desc("avgTemp"),
        desc("avgCo2"),
        desc("avgHumidity")
      )

    println("\nSort and group by average temperature, CO2, humidity, and country.\n")
    avgMetricsByCountry.show(false)

  }

  /**
   * Calculates the quantile value from a sorted array of Long values.
   *
   * This method calculates the quantile by determining the position in the
   * sorted array and interpolating between values if necessary.
   *
   * @param sortedArray The sorted array of Long values.
   * @param quantile The quantile to compute (e.g., 0.25 for the 25th percentile).
   * @return The quantile value.
   * @throws IllegalArgumentException if the array length is zero.
   */
  def getQuantile(sortedArray: Array[Long], quantile: Double): Double = {
    val n = sortedArray.length
    // Check if the array is empty
    if (n == 0) {
      throw new IllegalArgumentException("Array length must be greater than 0")
    }

    // Calculate the position in the array
    val pos = (n - 1) * quantile
    val lower = pos.floor.toInt
    val upper = pos.ceil.toInt

    // Check if the position is an integer
    if (lower == upper) {
      // If the position is an integer, return the value at that position
      sortedArray(lower).toDouble
    } else {
      // If the position is not an integer, interpolate between the two nearest values
      sortedArray(lower) * (upper - pos) + sortedArray(upper) * (pos - lower)
    }
  }

  /**
   * Utility function to calculate min and max values from a given dataset
   *
   * This function maps a given selector function over the dataset to
   * extract the values to be compared, and then uses a reduction
   * operation to find the minimum and maximum values.
   *
   * @param dataset The dataset from which to calculate the min and max values.
   * @param selector A function to select the value to be compared from each element of the dataset.
   * @tparam T The type of elements in the dataset.
   * @return A tuple containing the minimum and maximum values.
   */
  def calculateMinMax[T](dataset: Dataset[T], selector: T => Long): (Long, Long) = {
    import dataset.sparkSession.implicits._

    val minValue = dataset
      .map(selector)
      .reduce((value1, value2) => if (value1 < value2) value1 else value2)

    val maxValue = dataset
      .map(selector)
      .reduce((value1, value2) => if (value1 > value2) value1 else value2)

    // Return a tuple containing the min and max values
    (minValue, maxValue)
  }
}