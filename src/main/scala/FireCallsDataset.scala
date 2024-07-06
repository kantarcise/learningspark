package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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

      // Define schema - if we dont, we will get an error
      // Because when Spark loads data from CSV
      // file, and it infers all the fields as StringType by default.
      // In other words, when without a defined schema or inferSchema,
      // Spark treats all columns as StringType by default.
      val fireSchema = StructType(
        Array(
          StructField("CallNumber", IntegerType, nullable = true),
          StructField("UnitID", StringType, nullable = true),
          StructField("IncidentNumber", IntegerType, nullable = true),
          StructField("CallType", StringType, nullable = true),
          StructField("CallDate", StringType, nullable = true),
          StructField("WatchDate", StringType, nullable = true),
          StructField("CallFinalDisposition", StringType, nullable = true),
          StructField("AvailableDtTm", StringType, nullable = true),
          StructField("Address", StringType, nullable = true),
          StructField("City", StringType, nullable = true),
          StructField("Zipcode", IntegerType, nullable = true),
          StructField("Battalion", StringType, nullable = true),
          StructField("StationArea", StringType, nullable = true),
          StructField("Box", StringType, nullable = true),
          StructField("OriginalPriority", StringType, nullable = true),
          StructField("Priority", StringType, nullable = true),
          StructField("FinalPriority", IntegerType, nullable = true),
          StructField("ALSUnit", BooleanType, nullable = true),
          StructField("CallTypeGroup", StringType, nullable = true),
          StructField("NumAlarms", IntegerType, nullable = true),
          StructField("UnitType", StringType, nullable = true),
          StructField("UnitSequenceInCallDispatch", IntegerType, nullable = true),
          StructField("FirePreventionDistrict", StringType, nullable = true),
          StructField("SupervisorDistrict", StringType, nullable = true),
          StructField("Neighborhood", StringType, nullable = true),
          StructField("Location", StringType, nullable = true),
          StructField("RowID", StringType, nullable = true),
          StructField("Delay", DoubleType, nullable = true)
        )
      )

      // Read the CSV file into a Dataset
      val fireCallsDS = spark.read
        .format("csv")
        .option("header", value = true)
        .schema(fireSchema)
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
