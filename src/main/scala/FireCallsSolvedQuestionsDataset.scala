package learningSpark

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, functions => F}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.text.SimpleDateFormat
import java.sql.Timestamp

// a lot of intermediate case classes
case class NoMedicalFire(IncidentNumber: Option[Int],
                         AvailableDtTm: Option[String],
                         CallType: Option[String])

case class CallTypes(CallType: Option[String])

case class FireCallTransformed(ResponseDelayedinMins: Double)

case class OnlyTimestamps(IncidentDate: Option[Timestamp],
                          OnWatchDate: Option[Timestamp],
                          AvailableDtTS: Option[Timestamp])

case class TypesAndCountsOfCalls(CallType: String, count: Long)

// Define a case class to hold the aggregation results
case class AggregationResults(totalAlarms: Long,
                              averageDelayInMins: Double,
                              minimumDelayInMins: Double,
                              maximumDelayInMins: Double
                             )


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

    fireCallsDS
      .write
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
      .table(parquetTable)
      .as[FireCallInstance]

    val noMedicalSmallFireDS = fireCallsDS
      // .filter(_.CallType != Some("Medical Incident"))
      // Intellij suggests this one
      //.filter(!_.CallType.contains("Medical Incident"))
      // or explicitly
      .filter(m => !m.CallType.contains("Medical Incident"))
      // map to NoMedicalFire case class
      .map(calls => NoMedicalFire(calls.IncidentNumber, calls.AvailableDtTm, calls.CallType))

    println("No medical incidents\n")
    noMedicalSmallFireDS.show(5, truncate = false)

    val distinctCallTypes = fireCallsDS
      // .filter(_.CallType.isDefined)
      // This might be better for understanding
      .filter(ct => ct.CallType.isDefined)
      // let's not use Dataframe API!
      // .select($"CallType")
      // Transform to Dataset[String] by extracting the CallType
      .map(_.CallType.get)
      // Get distinct CallType values
      .distinct()
      // Count the distinct CallType values
      .count()

    println(s"Distinct CallTypes: $distinctCallTypes\n")

    // this was dataframe API
    // val newFireDS = fireCallsDS
      // Dont use Dataframe API!
      // .withColumnRenamed("Delay", "ResponseDelayedinMins")
      // .filter($"ResponseDelayedinMins" > 5)
      // .select("ResponseDelayedinMins")

    println("distinct CallTypes without null\n")

    fireCallsDS
      .filter(call => call.CallType.isDefined)
      .map(call => CallTypes(call.CallType))
      .distinct()
      .show()

    val newFireDS = fireCallsDS
      // rename the col by mapping!
      // Handle Option and provide default value - because we have Option[Double] in Delay!
      .map(call => FireCallTransformed(call.Delay.getOrElse(0.0)))
      .filter(firecall => firecall.ResponseDelayedinMins > 5.0)

    println("ResponseDelayedinMins \n")
    newFireDS.show(truncate = false)

    // this was dataframe API
    // val fireTsDS = fireCallsDS
    //   .withColumnRenamed("Delay", "ResponseDelayedinMins")
    //   .withColumn("IncidentDate", to_timestamp($"CallDate", "MM/dd/yyyy"))
    //   .withColumn("OnWatchDate", to_timestamp($"WatchDate", "MM/dd/yyyy"))
    //   .withColumn("AvailableDtTS", to_timestamp($"AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
    //   // Careful - This will error!
    //   // .drop("CallDate", "WatchDate", "AvailableDtTm")
    //   .drop($"CallDate", $"WatchDate", $"AvailableDtTm")
    //   // Our new case class, with timestamps
    //   .as[FireCallInstanceWithTimestamps]

    // Date format for conversion
    val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
    val timestampFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a")

    // let's make a Firecall dataset with timestamps!
    val fireTsDS = fireCallsDS.map { call =>
      FireCallInstanceWithTimestamps(
        call.CallNumber,
        call.UnitID,
        call.IncidentNumber,
        call.CallType,
        call.CallFinalDisposition,
        call.Address,
        call.City,
        call.ZipCode,
        call.Battalion,
        call.StationArea,
        call.Box,
        call.OriginalPriority,
        call.Priority,
        call.FinalPriority,
        call.ALSUnit,
        call.CallTypeGroup,
        call.NumAlarms,
        call.UnitType,
        call.UnitSequenceInCallDispatch,
        call.FirePreventionDistrict,
        call.SupervisorDistrict,
        call.Neighborhood,
        call.Location,
        call.RowID,
        call.Delay,  // Renamed to ResponseDelayedinMins
        call.CallDate.map(d => new Timestamp(dateFormat.parse(d).getTime)),
        call.WatchDate.map(d => new Timestamp(dateFormat.parse(d).getTime)),
        call.AvailableDtTm.map(d => new Timestamp(timestampFormat.parse(d).getTime))
      )
    }

    println("Fire DS is now with timestamps, showing only timestamps \n")
    fireTsDS
      // map to OnlyTimestamps case class to show
      .map(call => OnlyTimestamps(call.IncidentDate,
        call.OnWatchDate,
        call.AvailableDtTS))
      // .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, truncate = false)

    // this is done by Dataframe API
    // val maxDate: Timestamp = fireTsDS
    // .select(max($"IncidentDate"))
    // .take(1)(0)
    // .getTimestamp(0)

    // With dataset API
    val maxDate2: Timestamp = fireTsDS
      // Filter out undefined dates
      .filter(_.IncidentDate.isDefined)
      // Map to extract the defined IncidentDate
      .map(_.IncidentDate.get)
      // Reduce to find the maximum date
      .reduce((date1, date2) => if (date1.after(date2)) date1 else date2)


    val sevenDaysAgo: Timestamp = new Timestamp(maxDate2.getTime - 7L * 24 * 60 * 60 * 1000)

    val lastSevenDaysDS: Dataset[FireCallInstanceWithTimestamps] = fireTsDS
      .filter(call => call.IncidentDate.isDefined && call.IncidentDate.get.after(sevenDaysAgo))
      // .filter($"IncidentDate" >= date_sub(lit(maxDate), 7))
      // we cannot use this:
      //.filter(call => call.IncidentDate >= date_sub(lit(maxDate), days = 7))
      // Because filter function in the Dataset API expects a predicate
      // that can be evaluated on each element of the Dataset, but the
      // condition call.IncidentDate >= date_sub(lit(maxDate), days = 7) involves
      // a Spark SQL function which cannot be directly applied in this context.

    val callCount = lastSevenDaysDS.count()

    println(s"Number of calls logged in the last seven days: $callCount\n")

    println("How many years of FireCalls data do we have?\n")

    // this was Dataframe API
    // fireTsDS
    //   .select(year($"IncidentDate"))
    //   .distinct()
    //   .orderBy(year($"IncidentDate"))
    //   .show()

    fireTsDS
      // Filter out records where IncidentDate is not defined
      .filter(_.IncidentDate.isDefined)
      // Extract the year as an Int
      .map(call => call.IncidentDate.get.toLocalDateTime.getYear)
      .distinct()
      .sort("value")
      .show()

    println("The most common types of fire calls\n")
    fireTsDS
      .filter(_.CallType.isDefined)
      // Map to TypesAndCountsOfCalls directly
      .map(call => TypesAndCountsOfCalls(call.CallType.get, 1))
      .groupByKey(call => call.CallType)
      // Sum counts for each CallType
      .mapGroups((callType, iter) => TypesAndCountsOfCalls(callType, iter.map(_.count).sum))
      // Order by count in descending order
      .orderBy(desc("count"))
      .show(10)

    println("Some calculations - min - max - avg\n")

    // This was Dataframe API
    // fireTsDS
    //   .agg(
    //     F.sum("NumAlarms").alias("Total Alarms"),
    //     F.avg("ResponseDelayedinMins").as("Average Delay in Mins"),
    //     F.min("ResponseDelayedinMins").alias("Minimum Delay in Mins"),
    //     F.max("ResponseDelayedinMins").alias("Maximum Delay in Mins")
    //   )
    //   .show()

    // The Dataset API does not have direct equivalents for
    // the DataFrame agg method, so we need to perform the
    // aggregations using Scala's collection operations.

    // Perform the aggregation using reduce and map
    val aggregationResult = fireTsDS
      .filter(row => row.NumAlarms.isDefined && row.ResponseDelayedinMins.isDefined)
      .map(row => (
        row.NumAlarms.get,
        row.ResponseDelayedinMins.get,
        row.ResponseDelayedinMins.get,
        row.ResponseDelayedinMins.get,
        1
      ))
      .reduce((acc, value) => (
        // Sum of NumAlarms
        acc._1 + value._1,
        // Sum of ResponseDelayedinMins for average calculation
        acc._2 + value._2,
        // Minimum ResponseDelayedinMins
        Math.min(acc._3, value._3),
        // Maximum ResponseDelayedinMins
        Math.max(acc._4, value._4),
        // Count of records for average calculation
        acc._5 + value._5
      ))

    val totalAlarms = aggregationResult._1
    val averageDelayInMins = aggregationResult._2 / aggregationResult._5
    val minimumDelayInMins = aggregationResult._3
    val maximumDelayInMins = aggregationResult._4

    val result = Seq(
      AggregationResults(totalAlarms,
      averageDelayInMins,
      minimumDelayInMins,
      maximumDelayInMins))
      .toDS()

    result.show()


  }
}
