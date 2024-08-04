package learningSpark

import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.sql.Timestamp

/**
 * Let's try to solve the same questions that we've
 * answered in FireCallsSolvedQuestions
 *
 * But this time, with typed transformations only!
 *
 */
object FireCallsSolvedQuestionsDataset {

  // Let's scope some case classes that we will use!
  // there will be a lot of intermediate case classes
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

    val fireCallsDS = spark
      .read
      .option("header", "true")
      .schema(implicitly[Encoder[FireCallInstance]].schema)
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
    // Careful! If you run this code again, it will error,
    // saying the file already exists.
    val parquetTable = "fireCallTableFromDataset"
    fireCallsDS.write
      .format("parquet")
      .saveAsTable(parquetTable)

    // And read it back!
    val readBackDS: Dataset[FireCallInstance] = spark
      .table(parquetTable)
      .as[FireCallInstance]

    val noMedicalSmallFireDS = fireCallsDS
      // .filter(_.CallType != Some("Medical Incident"))
      // Intellij suggests this one
      //.filter(!_.CallType.contains("Medical Incident"))
      // or explicitly
      .filter(m => !m.CallType.contains("Medical Incident"))
      // map to NoMedicalFire case class
      .map(calls => NoMedicalFire(calls.IncidentNumber,
        calls.AvailableDtTm, calls.CallType))

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

    println("distinct CallTypes without null\n")

    fireCallsDS
      .filter(call => call.CallType.isDefined)
      .map(call => CallTypes(call.CallType))
      .distinct()
      .show()

    val newFireDS = fireCallsDS
      // rename the col by mapping!
      // Handle Option and provide default value - because we
      // have Option[Double] in Delay!
      .map(call => FireCallTransformed(call.Delay.getOrElse(.0)))
      .filter(firecall => firecall.ResponseDelayedinMins > 5.0)

    println("ResponseDelayedinMins \n")
    newFireDS
      .show(truncate = false)

    // Date format for conversion
    val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
    val timestampFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a")

    // let's make a Firecall dataset with timestamps!
    val fireTsDS = fireCallsDS
      .map(call => mapFireCallsToFireCallsTimestamp(call, dateFormat, timestampFormat))

    println("Fire DS is now with timestamps, showing only timestamps \n")
    fireTsDS
      // map to OnlyTimestamps case class to show
      .map(call => OnlyTimestamps(
        call.IncidentDate,
        call.OnWatchDate,
        call.AvailableDtTS))
      .show(5, truncate = false)

    // With dataset API
    val maxDate2: Timestamp = fireTsDS
      // Filter out undefined dates, null filter
      .filter(_.IncidentDate.isDefined)
      // Map to extract the defined IncidentDate
      .map(_.IncidentDate.get)
      // Reduce to find the maximum date
      .reduce((date1, date2) => if (date1.after(date2)) date1 else date2)

    val sevenDaysAgo: Timestamp = new Timestamp(maxDate2.getTime - 7L * 24 * 60 * 60 * 1000)

    val lastSevenDaysDS: Dataset[FireCallInstanceWithTimestamps] = fireTsDS
      .filter(call => call.IncidentDate.isDefined &&
        call.IncidentDate.get.after(sevenDaysAgo))
      // we cannot use this:
      //.filter(call => call.IncidentDate >= date_sub(lit(maxDate), days = 7))
      // Because filter function in the Dataset API expects a predicate
      // that can be evaluated on each element of the Dataset, but the
      // condition call.IncidentDate >= date_sub(lit(maxDate), days = 7) involves
      // a Spark SQL function which cannot be directly applied in this context.

    val callCount = lastSevenDaysDS.count()

    println(s"Number of calls logged in the last seven days: $callCount\n")

    println("How many years of FireCalls data do we have?\n")

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
        // 1 for counting the number of records
        1
      ))
      // an accumulator and a value
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

    // now we have calculated what we are looking for,
    // we can just use it!
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

  /**
   * A simple mapping method to generate
   * FireCallInstanceWithTimestamps instances
   * @param call
   * @param dateFormat
   * @param timestampFormat
   * @return
   */
  def mapFireCallsToFireCallsTimestamp(call: FireCallInstance,
                     dateFormat: SimpleDateFormat,
                     timestampFormat: SimpleDateFormat): FireCallInstanceWithTimestamps = {
    FireCallInstanceWithTimestamps(
      call.CallNumber,
      call.UnitID,
      call.IncidentNumber,
      call.CallType,
      call.CallFinalDisposition,
      call.Address,
      call.City,
      call.Zipcode,
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
}
