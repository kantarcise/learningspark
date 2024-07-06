package learningSpark

import org.apache.spark.sql.SparkSession

// Let's discover how to effectively use the Dataset - Dataframe API
object IoTDatasetFunctionComparison {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("IoTDatasetFunctionComparison")
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

    // THIS IS A DATASET NOW, thanks to Case Class
    val iotDS = spark
      .read
      .json(iotFilePath)
      .as[DeviceIoTData]

    println("This is a IoT Devices Dataset, made with case Classes\n")
    iotDS.show(5, truncate = false)

    println("Filter Temperatures with Lambdas\n")
    val filterTempDs = iotDS
      .filter({d => d.temp > 30 && d.humidity > 70})

    filterTempDs.show(5, truncate = false)

    // More Information
    // Page 170 in the book Learning Spark
    println("Filter Temperatures with DSL\n")
    val filterTempDsDSL = iotDS
      .filter($"temp" > 30)
      .filter($"humidity" > 70)

    filterTempDsDSL.show(5, truncate = false)

    println("Here comes the good stuff")

    println("Let's compare Lambdas vs DSL in Dataframes and Datasets\n")

    println("Here is a small part of Dataset as a Dataframe, where temp > 25\n")
    val startTimeLambdaTempDF = System.nanoTime()
    // this is a DataFrame
    val tempDF = iotDS
      .filter(d => {d.temp > 25})
      .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3")

    val endTimeLambdaTempDF = System.nanoTime()
    val durationLambdaTempDF = (endTimeLambdaTempDF - startTimeLambdaTempDF) / 1e9d // convert to seconds

    // 0.033 seconds
    println(f"Duration with LAMBDA ONLY in Dataframes: $durationLambdaTempDF%.3f seconds\n")

    tempDF.show(truncate = false)

    println("Small part of Dataset as a Dataframe, where temp > 25")
    println("Now without lambdas\n")
    val startTimeDSLTempDF = System.nanoTime()
    val tempDFDSL = iotDS
      .filter($"temp" > 25)
      .select($"temp", $"device_name", $"device_id", $"cca3")
    val endTimeDSLTempDF = System.nanoTime()
    val durationTimeDSLTempDF = (endTimeDSLTempDF - startTimeDSLTempDF) / 1e9d // convert to seconds

    // 0.010 seconds
    println(f"Duration with DSL only in Dataframes: $durationTimeDSLTempDF%.3f seconds\n")

    tempDFDSL.show(truncate = false)

    println("Dataset - With Lambdas\n")

    val startTimeLambdaTempDS = System.nanoTime()
    val tempDS = iotDS
      .filter(d => {d.temp > 25})
      .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3")
      .as[DeviceTempByCountry]

    val endTimeLambdaTempDS = System.nanoTime()
    val durationTimeLambdaTempDS = (endTimeLambdaTempDS - startTimeLambdaTempDS) / 1e9d // convert to seconds

    // 0.029 seconds
    println(f"Duration with LAMBDA ONLY in Datasets: $durationTimeLambdaTempDS%.3f seconds\n")
    tempDS.show(truncate = false)

    println("Dataset - Now without Lambdas\n")

    val startTimeDSLTempDS = System.nanoTime()
    val tempDSDSL = iotDS
      .filter($"temp" > 25)
      .select($"temp", $"device_name", $"device_id", $"cca3")
      .as[DeviceTempByCountry]

    val endTimeDSLTempDS = System.nanoTime()
    val durationTimeDSLTempDS = (endTimeDSLTempDS - startTimeDSLTempDS) / 1e9d // convert to seconds

    // 0.014 seconds
    println(f"Duration with DSL only in Dataframes: $durationTimeDSLTempDS%.3f seconds\n")

    tempDSDSL.show(truncate = false)

    // Or you can inspect only the first row of your Dataset:
    // using first()
    val device = tempDSDSL.first()
    println(s"first row of the dataset $device")
  }
}
