package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Same example, with Datasets
object FireCallsDataset {

    def main(args: Array[String]): Unit = {

      val spark = SparkSession
        .builder
        .appName("sparkApp")
        .master("local[*]")
        .getOrCreate()

      // log level - verbose = False
      spark.sparkContext.setLogLevel("ERROR")

      // $ usage
      import spark.implicits._

      // Define the data path as a val
      val fireCallsPath: String = {
        val projectDir = System.getProperty("user.dir")
        s"$projectDir/data/sf-fire-calls.csv"
      }

      // Read the CSV file into a Dataset
      val fireCallsDS = spark.read
        .format("csv")
        .option("header", value = true)
        .option("inferSchema", value = true)
        .load(fireCallsPath)
        .as[FireCallInstance]

      println("\nThe schema before, for fireCalls DS \n")
      fireCallsDS.printSchema()

      // Make the new column
      // from string to timestamp
      val updatedFireCallsDS = fireCallsDS
        .withColumn("CallDateTimestamp", unix_timestamp($"CallDate", "dd/MM/yyyy").cast("timestamp"))
        .withColumn("CallDateTimestampTwo", to_timestamp($"CallDate", "dd/MM/yyyy"))

      // Now perform select and filter
      val selection = updatedFireCallsDS
        .select("CallDate", "StationArea", "ZipCode", "CallDateTimestamp", "CallDateTimestampTwo")
        .filter($"CallDateTimestamp".isNotNull)
        .filter($"CallDateTimestampTwo".isNotNull)

      val orderedSelection = selection
        .where("ZipCode = 94118")
        .orderBy("CallDateTimestamp")
        .select("CallDateTimestamp", "StationArea", "ZipCode", "CallDateTimestampTwo")

      println("\n Ordered and filtered fireCalls DS, " +
        "with type casted from string to timestamp \n")
      orderedSelection.show(20)

      println("\nThe schema after casting \n")
      orderedSelection.printSchema()

      spark.stop()
    }
}
