package learningSpark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration

// there was an option to typed aggregations, but now deprecated.
// import org.apache.spark.sql.expressions.scalalang.typed


/**
There are some Stateful Aggregations which are done without timing.
This is an example application to demonstrate.
 */
object ManagedStatefulAggregationsWithoutTime {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("StatefulOperations")
      // if you want to run it in IDE
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // let's make a memory stream of sensor data
    // make a memory stream to test the Stateless Operations
    val memoryStream: MemoryStream[DeviceIoTData] = new MemoryStream[DeviceIoTData](id = 1, spark.sqlContext)

    // Add sample data every 5 seconds - one by one!
    val addDataFuture = addDataPeriodicallyToMemoryStream(memoryStream, 5.seconds)

    // Make a streaming Dataset from the memory stream
    val sensorStream: Dataset[DeviceIoTData] = memoryStream
      .toDS()

    // there are Global aggregations
    // we cannot use sensorStream.count()
    // because for streaming DataFrames/Datasets
    // aggregates have to be continuously updated
    // THIS IS DATAFRAME API!
    // it will increase one by one
    val runningCount: DataFrame = sensorStream
      .groupBy()
      .count()

    // and there are Grouped aggregations
    // let's have a running count based on sensor countries!
    val runningCountOfCountries = sensorStream
      .groupByKey(sensor => sensor.cca3)
      .count()

    // here is another grouped aggregation
    // let's see the minimum temperature for different countries
    val minTemperatureByCountry = sensorStream
      .groupByKey(s => s.cca3)
      .agg(min($"temp").as[Long])
      // careful! if you use like down below, you will get a Dataframe
      // .agg(min($"temperature").as("min_temperature"))

    // you can check out different types of aggregations here
    allTypesOfAggregations(sensorStream.toDF())

    val query = runningCount
      .writeStream
      // Append output mode not supported when there are streaming
      // aggregations on streaming DataFrames/DataSets without watermark;
      // .outputMode("append")
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .start()

    val querySecond = runningCountOfCountries
      .writeStream
      .outputMode("complete")
      .format("console")
      // if we do not configure it
      // default trigger is just micro batches
      // .trigger(Trigger.ProcessingTime("1 seconds"))
      .start()

    val queryThird = minTemperatureByCountry
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
    querySecond.awaitTermination()
    queryThird.awaitTermination()

    // Wait for the data adding to finish (it won't, but in a real
    // use case you might want to manage this better)
    Await.result(addDataFuture, Duration.Inf)
  }

  /**
   Add generated data on time interval, to an existing MemoryStream.
   This simulates a real streaming scenario where data arrives continuously.
   */
  def addDataPeriodicallyToMemoryStream(memoryStream: MemoryStream[DeviceIoTData],
                                        interval: FiniteDuration): Future[Unit] = Future {

    val testData: Seq[DeviceIoTData] = Seq(
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
    )

    testData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }
  }

  /**
  Here is some types of built in aggregations
  To discover more:
  https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html
   */
  def allTypesOfAggregations(df: DataFrame): Unit = {

    // mean value of c02_level
    df.groupBy("cca2").mean("c02_level")

    // sum of battery levels
    df.groupBy("cca2").sum("battery_level")

    // approximate number of distinct items in a group device_id
    df.groupBy("device_id").agg(approx_count_distinct("battery_level"))

    // returns the first value of a column in a group
    // which are the first devices to work in different countries?
    // we can either give col name as a string, like "device_name"
    // or we can give is as col("device_name") / $"device_name"
    df.groupBy("cca2").agg(first(col("device_name")))

    // returns the sample standard deviation of the expression in a group.
    // if we do not select any groups, it will run global
    df.agg(stddev(col("temp")).as("sample_stddev_temperature"))

    // we can even do multiple aggregations together!
    // count all incoming
    // mean of c02 level
    // collect countries in a set
    val multipleAggs = df
      .groupBy("device_id")
      .agg(count("*"), mean("c02_level").alias("c02baselineValue"),
        collect_set("cca3").alias("allCountries"))
  }
}