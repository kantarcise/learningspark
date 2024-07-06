package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

// same exercises, using Dataset API
object FireCallsSolvedQuestionsDataset {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("FireCallsSolvedQuestionsDataset")
      .master("local[*]")
      .getOrCreate()

    // verbose = False
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val fireCallsPath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-fire-calls.csv"
    }

    // Define Schema
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

    val fireCallsDS = spark
      .read
      .option("header", "true")
      .schema(fireSchema)
      .csv(fireCallsPath)
      .as[FireCallInstance]

    println("\n fireCallsDS loaded \n")
    fireCallsDS.show(5)

    // We can save as parquet no problem!
    val parquetPath = "/tmp/parquet2"

    fireCallsDS.write
      .mode(SaveMode.Overwrite)
      .parquet(parquetPath)

    println("\n fireCallsDS saved as parquet\n")

    // We can write our Dataset as table too!
    // Careful! If you run this code again, it will error, saying the file already exists.
    val parquetTable = "fireCallTableFromDataset"
    fireCallsDS.write
      .format("parquet")
      .saveAsTable(parquetTable)

    // And read it back!
    val readBackDS = spark
      .table(parquetTable).as[FireCallInstance]

    val noMedicalSmallFireDS = fireCallsDS
      // .filter(_.CallType != Some("Medical Incident"))
      // Intellij suggests this one
      .filter(!_.CallType.contains("Medical Incident"))
      .select("IncidentNumber", "AvailableDtTm", "CallType")

    println("No medical incidents\n")
    noMedicalSmallFireDS.show(5, truncate = false)

    val distinctCallTypes = fireCallsDS
      .filter(_.CallType.isDefined)
      .select($"CallType")
      .distinct()
      .count()

    println(s"Distinct CallTypes: $distinctCallTypes\n")

    val newFireDS = fireCallsDS
      .withColumnRenamed("Delay", "ResponseDelayedinMins")
      .filter($"ResponseDelayedinMins" > 5)
      .select("ResponseDelayedinMins")

    println("ResponseDelayedinMins \n")
    newFireDS.show(truncate = false)

    val fireTsDS = fireCallsDS
      .withColumnRenamed("Delay", "ResponseDelayedinMins")
      .withColumn("IncidentDate", to_timestamp($"CallDate", "MM/dd/yyyy"))
      .withColumn("OnWatchDate", to_timestamp($"WatchDate", "MM/dd/yyyy"))
      .withColumn("AvailableDtTS", to_timestamp($"AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
      // Careful - This will error!
      // .drop("CallDate", "WatchDate", "AvailableDtTm")
      .drop($"CallDate", $"WatchDate", $"AvailableDtTm")
      // Our new case class, with timestamps
      .as[FireCallInstanceWithTimestamps]

    println("Fire DS now with timestamps, only timestamps selected \n")
    fireTsDS
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, truncate = false)

    val maxDate = fireTsDS
      .select(max($"IncidentDate"))
      .take(1)(0)
      .getTimestamp(0)

    val lastSevenDaysDS = fireTsDS
      .filter($"IncidentDate" >= date_sub(lit(maxDate), 7))

    val callCount = lastSevenDaysDS.count()

    println(s"Number of calls logged in the last seven days: $callCount")

    println("How many years of FireCalls data do we have?\n")
    fireTsDS
      .select(year($"IncidentDate"))
      .distinct()
      .orderBy(year($"IncidentDate"))
      .show()

    println("The most common types of fire calls\n")
    fireTsDS
      .filter(_.CallType.isDefined)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10)

    println("Some calculations - min - max - avg\n")
    fireTsDS
      .agg(
        F.sum("NumAlarms").alias("Total Alarms"),
        F.avg("ResponseDelayedinMins").as("Average Delay in Mins"),
        F.min("ResponseDelayedinMins").alias("Minimum Delay in Mins"),
        F.max("ResponseDelayedinMins").alias("Maximum Delay in Mins")
      )
      .show()
  }
}
