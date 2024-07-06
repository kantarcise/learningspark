package learningSpark


import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

class IotDatasetWorkoutTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession
    .builder
    .appName("IotDatasetWorkoutTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val testData: Dataset[DeviceIoTData] = Seq(
    DeviceIoTData(123, "device-mac-123zvY7uWFB", "208.250.26.135", "US", "USA", "United States", 38.0, -97.0, "Celsius", 29, 33, 0, 1520, "red", 1458444054199L),
    DeviceIoTData(124, "sensor-pad-124b8sCFB", "72.34.37.168", "US", "USA", "United States", 34.04, -118.25, "Celsius", 26, 64, 7, 1360, "yellow", 1458444054200L),
    DeviceIoTData(125, "therm-stick-125X3edirdM", "82.116.1.82", "RU", "RUS", "Russia", 61.25, 73.42, "Celsius", 17, 87, 1, 1206, "yellow", 1458444054200L),
    DeviceIoTData(126, "sensor-pad-1269atdM0jAk", "222.98.55.1", "KR", "KOR", "Republic of Korea", 37.57, 126.98, "Celsius", 31, 27, 9, 918, "green", 1458444054201L),
    DeviceIoTData(127, "meter-gauge-127jwKuJMYb90", "219.103.112.2", "JP", "JPN", "Japan", 35.69, 139.69, "Celsius", 18, 40, 3, 1007, "yellow", 1458444054202L),
    DeviceIoTData(128, "sensor-pad-128FCJAk99Zr", "217.180.15.171", "GB", "GBR", "United Kingdom", 51.5, -0.13, "Celsius", 30, 68, 1, 1193, "yellow", 1458444054203L),
    DeviceIoTData(129, "device-mac-129noCEfOX", "202.162.38.90", "ID", "IDN", "Indonesia", -7.78, 110.36, "Celsius", 11, 95, 2, 1405, "red", 1458444054203L),
    DeviceIoTData(130, "sensor-pad-130QcD7KHEM", "63.218.164.50", "US", "USA", "United States", 38.98, -77.38, "Celsius", 29, 27, 7, 860, "green", 1458444054204L),
    DeviceIoTData(131, "meter-gauge-131LbuCgFDFfz", "207.165.237.193", "US", "USA", "United States", 43.15, -93.21, "Celsius", 16, 68, 0, 1133, "yellow", 1458444054204L),
    DeviceIoTData(132, "sensor-pad-1325DdN3NiTBj", "194.126.113.134", "EE", "EST", "Estonia", 59.0, 26.0, "Celsius", 26, 71, 5, 1069, "yellow", 1458444054205L),
    DeviceIoTData(133, "meter-gauge-133ReblmhHY", "109.235.180.1", "CZ", "CZE", "Czech Republic", 50.08, 14.42, "Celsius", 27, 84, 2, 929, "green", 1458444054205L),
    DeviceIoTData(134, "sensor-pad-1344UGR6ipMd", "65.103.30.102", "US", "USA", "United States", 38.0, -97.0, "Celsius", 23, 76, 0, 804, "green", 1458444054206L),
    DeviceIoTData(135, "device-mac-135kwwdl", "195.66.190.222", "ME", "MNE", "Montenegro", 42.44, 19.26, "Celsius", 16, 84, 3, 875, "green", 1458444054206L),
    DeviceIoTData(136, "sensor-pad-1362BAqBEt", "211.161.47.10", "CN", "CHN", "China", 39.93, 116.39, "Celsius", 26, 82, 5, 1477, "red", 1458444054207L)
  ).toDS()

  test("Detecting devices with battery levels below a threshold") {
    val maxBatteryLevel = testData
      .agg(max("battery_level"))
      .first()
      .getLong(0)

    // we know that
    assert(maxBatteryLevel == 9)

    val batteryThreshold = maxBatteryLevel * 0.20

    assert(batteryThreshold == 1.8)

    val lowBatteryDs = testData.filter($"battery_level" < batteryThreshold)

    // we have 5 low battery devices
    assert(lowBatteryDs.count() == 5)
  }

  test("Identifying offending countries with high levels of CO2 emissions") {

    val co2quantiles = testData
      .stat
      .approxQuantile("c02_level", Array(0.25, 0.75), 0.0)

    val co2Threshold75th = co2quantiles(1)

    val offendingCountries = testData.filter($"c02_level" > co2Threshold75th)

    // we can see that US, Indonesia and CN is over threshold in c02
    val expectedCountries = Set("United States", "Indonesia", "China")

    val resultCountries = offendingCountries
      .select("cn")
      .as[String]
      .collect()
      .toSet

    assert(resultCountries == expectedCountries)
  }

  test("Computing the min and max values for temperature, battery level, CO2, and humidity") {

    val minTemp = testData.agg(min("temp")).first().getLong(0)
    val maxTemp = testData.agg(max("temp")).first().getLong(0)
    val minBatteryLevel = testData.agg(min("battery_level")).first().getLong(0)
    val maxBatteryLevel = testData.agg(max("battery_level")).first().getLong(0)
    val minCO2 = testData.agg(min("c02_level")).first().getLong(0)
    val maxCO2 = testData.agg(max("c02_level")).first().getLong(0)
    val minHumidity = testData.agg(min("humidity")).first().getLong(0)
    val maxHumidity = testData.agg(max("humidity")).first().getLong(0)

    // from our test set, we can confirm these all
    assert(minTemp == 11 && maxTemp == 31)
    assert(minBatteryLevel == 0 && maxBatteryLevel == 9)
    assert(minCO2 == 804 && maxCO2 == 1520)
    assert(minHumidity == 27 && maxHumidity == 95)
  }

  test("Sorting and grouping by average temperature, CO2, humidity, and country") {

    // let's calculate the averate metrics for our test data
    val avgMetricsByCountry = testData
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

    // here is the result, mapped to a set, because all of countries are unique
    val result = avgMetricsByCountry
      .collect()
      .map(row => (row.getString(0), row.getDouble(1), row.getDouble(2), row.getDouble(3)))
      .toSet

    // for each country, we can see confirm these stats
    val expected = Array(
      ("Republic of Korea", 31.0, 918.0, 27.0),
      ("United Kingdom", 30.0, 1193.0, 68.0),
      ("United States", 24.6, 1135.4, 53.6),
      ("China", 26.0, 1477.0, 82.0),
      ("Czech Republic", 27.0, 929.0, 84.0),
      ("Russia", 17.0, 1206.0, 87.0),
      ("Montenegro", 16.0, 875.0, 84.0),
      ("Japan", 18.0, 1007.0, 40.0),
      ("Indonesia", 11.0, 1405.0, 95.0),
      ("Estonia", 26.0, 1069.0, 71.0)
    ).toSet

    assert(result == expected)
  }
}