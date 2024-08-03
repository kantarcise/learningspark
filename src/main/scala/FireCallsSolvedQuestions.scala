package learningSpark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

// Here is another way to import
// kinda like python - `import numpy as np`
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types.{BooleanType, IntegerType,
  StringType, StructField, StructType}
import org.apache.spark.sql.SaveMode

/**
 * Let's answer some questions about our data
 * to analyze what we are working with here!
 *
 * There are 8 questions we are trying to answer.
 */
object FireCallsSolvedQuestions {

  // define schema for Dataframe
  val fireSchema: StructType = StructType(Array(
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
      // we can use the class name!
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Define the data path as a val
    val fireCallsPath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-fire-calls.csv"
    }

    val fireCallsDF = spark
      .read
      .format("csv")
      .option("header", value = true)
      .schema(fireSchema)
      .load(fireCallsPath)

    println("\n fireCallsDF loaded \n")
    fireCallsDF.show(5)

    // In Scala to save as a Parquet file
    // Assuming a Linux OS.
    val parquetPath = "/tmp/parquet"

    fireCallsDF
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(parquetPath)

    println("\n fireCallsDF saved as parquet\n")

    // We can save our Dataframe as a table
    // which will generate the files under project root:
    // /spark-warehouse/part...
    // Careful! If you run this code again, it will error, saying the file already exists.
    val parquetTable = "fireCallTable" // name of the table
    fireCallsDF
      .write
      .format("parquet")
      .saveAsTable(parquetTable)

    // we can run spark.sql() queries on this table - but
    // we will discover that later.
    // Also, we can read this table back to a Dataframe
    val readBackDF: DataFrame = spark
      .table(parquetTable)

    // 1- Find all incidents without medical ones.
    val noMedicalSmallFireDF = fireCallsDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") =!= "Medical Incident")

    println("No medical incidents\n")
    noMedicalSmallFireDF.show(5, truncate = false)

    // 2- What if we want to know how many distinct CallTypes
    // were recorded as the causes
    // of the fire calls?

    val distinctCallTypes = fireCallsDF
      .select($"CallType")
      .where($"CallType".isNotNull)
      //.agg(count("CallType").alias("NumberCallTypes"))
      .agg(countDistinct("CallType") as "DistinctCallTypes")

    println("distinct CallTypes\n")
    distinctCallTypes.show(truncate = false)
    // 30 in a dataframe

    println("distinct CallTypes without null\n")
    fireCallsDF
      .select(col("CallType"))
      .where(col("CallType").isNotNull)
      // return only unique rows
      .distinct()
      // you can also use
      // .dropDuplicates()
      .show()

    // 3- Let’s change the name of our Delay column to
    // `ResponseDelayedinMins` and take a look at the
    // response times that were longer than five minutes:

    val newFireDF = fireCallsDF.
      withColumnRenamed("Delay", "ResponseDelayedinMins")
      .select("ResponseDelayedinMins")
      .where(col("ResponseDelayedinMins") > 5)

    println("ResponseDelayedinMins \n ")
    newFireDF
      .show(truncate = false)

    // 4- String to timestamp with a Pattern
    // "MM/dd/yyyy" or "MM/dd/yyyy hh:mm:ss a"
    // Also let's drop not used cols
    val fireTsDF = fireCallsDF
      .withColumnRenamed("Delay", "ResponseDelayedinMins")
      .withColumn("IncidentDate", to_timestamp($"CallDate", "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp($"WatchDate", "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp($"AvailableDtTm",
        "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")

    // Select and show the converted columns
    println("Fire DF now with timestamps, only timestamps selected \n")
    fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, truncate = false)

    /*
    Now that we have modified the dates, we can query
    using functions from spark.sql.functions
    like month(), year(), and day() to explore
    our data further.

    We could find out how many calls were logged
    in the last seven days, or we could see how
    many years’ worth of Fire Department calls
    are included in the data set with this query:
    */

    // 5- Find last 7 days:
    // Calculate the number of calls logged in the last seven days
    // TODO : This is cool, but not useful - 
    //  because it is from current date
    val lastSevenDaysFromCurrentDate = fireTsDF
      .where(col("IncidentDate") >= date_sub(current_date(), 7))

    // Calculate the maximum date in IncidentDate
    // Todo THIS did not work
    // val maxDate = fireTsDF
    //  .agg(max(col("IncidentDate"))).as[TimestampType]

    // Calculate the maximum date in IncidentDate
    val maxDate = fireTsDF
      .select(max($"IncidentDate"))
      // collect ===  DANGEROUS -
      // Why ? - If you are working with huge
      // amounts of data, then the driver node might easily run out of memory.
      // take() action scans the first partition it finds and returns the result.
      //.collect()(0) // collect returns an array containing all rows
      .take(1)(0)
      // and get it as timestamp
      .getTimestamp(0)

    // Calculate the number of calls logged in the last seven
    // days from the max date
    val lastSevenDaysDF = fireTsDF
      // Creates a Column of literal value.
      .filter($"IncidentDate" >= date_sub(lit(maxDate), 7))

    // we can just count it!
    val callCount = lastSevenDaysDF
      .count()

    println(s"Number of calls logged in the last seven days: $callCount")

    // 6- How many years of FireCalls data do we have?
    println("How many years of FireCalls data do we have?\n")
    fireTsDF
      .select(year(col("IncidentDate")))
      .distinct()
      .orderBy(year(col("IncidentDate")))
      .show()

    // 7- What were the most common types of fire calls?
    // My approach
    println("My approach")
    println("The most common types of fire calls\n")
    fireTsDF
      .select(col("CallType"))
      .where(col("CallType").isNotNull)
      .groupBy(col("CallType"))
      .agg(count(col("CallType")) as "CallTypeCounts")
      .orderBy(desc("CallTypeCounts"))
      .show(10)

    println("Book approach")
    println("The most common types of fire calls\n")
    // book
    fireTsDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      // default, will make a col named count
      .count()
      .orderBy(desc("count"))
      .show(10)

    // 8- Compute the sum of alarms, the average response time,
    // and the minimum and maximum response times to all fire calls in our data:
    println("Some calculations - min - max - avg\n")
    fireTsDF
      .select(F.sum("NumAlarms").alias("Total Alarms"),
        F.avg("ResponseDelayedinMins") as "Average Delay in Mins",
        F.min("ResponseDelayedinMins").alias("Minimum Delay in Mins"),
        F.max("ResponseDelayedinMins").alias("Maximum Delay in Mins"))
      .show()

  }
}