package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Let's work on the /sf-fire-calls.csv
 *
 * And discover some common typecasting and
 * typed & untyped transformations.
 */
object FireCalls {

  // If we want to use Dataframe API, we kinda had to write this.
  // Because when Spark loads data from CSV
  // file, and it infers all the fields as StringType by default.
  // In other words, when without a defined schema or inferSchema,
  // Spark treats all columns as StringType by default.
  val fireSchema: StructType = StructType(
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

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Fire Calls Discovery!")
      .master("local[*]")
      .getOrCreate()

    // we can also select ERROR
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Define the data path as a val
    val fireCallsPath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-fire-calls.csv"
    }

    // get the data onto a Dataframe
    val fireCallsDF = spark.read
      .format("csv")
      .option("header", value = true)
      .schema(fireSchema)
      .load(fireCallsPath)

    println("\nThe schema before, for fireCalls DF \n")
    fireCallsDF.printSchema()

    // Let's make the new column from string to timestamp
    // both approaches will work
    val updatedFireCallsDF = fireCallsDF
      // With unix_timestamp
      .withColumn("CallDateTimestamp", unix_timestamp($"CallDate", "dd/MM/yyyy").cast(TimestampType))
      // With to_timestamp
      .withColumn("CallDateTimestampTwo", to_timestamp($"CallDate", "dd/MM/yyyy"))

    // Now perform select and filter
    val selection = updatedFireCallsDF
      .select("CallDate", "StationArea", "Zipcode", "CallDateTimestamp", "CallDateTimestampTwo")
      // both filters will work down below!
      // we can use the name of the Dataframe to access the col!
      .filter(updatedFireCallsDF("CallDateTimestamp").isNotNull)
      .filter($"CallDateTimestampTwo".isNotNull)

    val orderedSelection = selection
      .where("ZipCode = 94118")
      .orderBy("CallDateTimestamp")
      .select("CallDateTimestamp", "StationArea", "Zipcode", "CallDateTimestampTwo")

    println("\n Ordered and filtered fireCalls DF, " +
      "with type casted from string to timestamp \n")
    orderedSelection.show(20)

    println("\nThe schema after casting \n")
    orderedSelection.printSchema()

    spark.stop()
  }
}