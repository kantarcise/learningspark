package learningSpark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Let's discover how to effectively use the
 * Dataset - Dataframe API
 * meaning, typed and untyped transformations.
 *
 * In the book, at the end of Chapter 6, there is
 * a different experiment regarding
 * DSL and Lambda usage!
 */
object IoTDatasetFunctionComparison {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("IoTDatasetFunctionComparison")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val iotFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/iot_devices.json"
    }

    val iotDS: Dataset[DeviceIoTData] = spark
      .read
      .json(iotFilePath)
      .as[DeviceIoTData]

    val iotDF: DataFrame = spark
      .read
      .json(iotFilePath)

    // cache both
    iotDS.cache()
    iotDF.cache()

    println("This is a IoT Devices Dataset, made with case Classes\n")
    iotDS.show(5, truncate = false)

    println("First, let's understand the difference between DSL and Lambdas\n:")

    println("Filter Temperatures with Lambdas\n")
    val filterTempDs = iotDS
      .filter({d => d.temp > 30 && d.humidity > 70})

    filterTempDs.show(5, truncate = false)

    // More Information
    // Page 170 in the book Learning Spark
    println("Filter Temperatures with DSL\n")
    val filterTempDsDSL = iotDF
      .filter($"temp" > 30)
      .filter($"humidity" > 70)

    filterTempDsDSL.show(5, truncate = false)

    println("Now, let's compare Lambdas vs DSL in Dataframes and Datasets\n")

    println("Here is a small part of Dataset, where temp > 25\n")
    val startTimeLambdaTempDF = System.nanoTime()
    // this is a DataFrame
    val tempDF = iotDS
      .filter(d => {d.temp > 25})
      .map(d => DeviceTempByCountry(d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3")

    val endTimeLambdaTempDF = System.nanoTime()
    // convert to milliseconds
    val durationLambdaTempDF = (endTimeLambdaTempDF - startTimeLambdaTempDF) / 1e6d

    tempDF.show(truncate = false)

    // 0.033 seconds
    println(f"Duration with LAMBDA ONLY in Dataframes: $durationLambdaTempDF%.3f milliseconds\n")

    println("Small part of Dataset as a Dataframe, where temp > 25")
    println("Now without lambdas\n")
    val startTimeDSLTempDF = System.nanoTime()
    val tempDFDSL = iotDF
      .filter($"temp" > 25)
      .select($"temp", $"device_name", $"device_id", $"cca3")
    val endTimeDSLTempDF = System.nanoTime()

    // convert to milliseconds
    val durationTimeDSLTempDF = (endTimeDSLTempDF - startTimeDSLTempDF) / 1e6d

    tempDFDSL.show(truncate = false)

    // 0.010 seconds
    println(f"Duration with DSL only in Dataframes: $durationTimeDSLTempDF%.3f milliseconds\n")

    println("Dataset - With Lambdas\n")

    val startTimeLambdaTempDS = System.nanoTime()
    val tempDS = iotDS
      .filter(d => {d.temp > 25})
      .map(d => DeviceTempByCountry(d.temp, d.device_name, d.device_id, d.cca3))

    val endTimeLambdaTempDS = System.nanoTime()
    // convert to milliseconds
    val durationTimeLambdaTempDS = (endTimeLambdaTempDS - startTimeLambdaTempDS) / 1e6d

    // 0.029 seconds
    println(f"Duration with LAMBDA ONLY in Datasets: $durationTimeLambdaTempDS%.3f milliseconds\n")
    tempDS.show(truncate = false)

    println("Dataset - Now without Lambdas\n")

    val startTimeDSLTempDS = System.nanoTime()
    val tempDSDSL = iotDS
      .filter($"temp" > 25)
      .select($"temp", $"device_name", $"device_id", $"cca3")
      .as[DeviceTempByCountry]

    val endTimeDSLTempDS = System.nanoTime()
    val durationTimeDSLTempDS = (endTimeDSLTempDS - startTimeDSLTempDS) / 1e6d

    tempDSDSL.show(truncate = false)

    // 0.014 seconds
    println(f"Duration with DSL only in Datasets: $durationTimeDSLTempDS%.3f milliseconds\n")

    // Or you can inspect only the first row of your Dataset:
    // using first()
    val device = tempDSDSL.first()
    println(s"first row of the dataset $device")
  }
}
