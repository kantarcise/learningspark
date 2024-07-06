package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}
import org.scalatest.funsuite.AnyFunSuite

class FireCallsSolvedQuestionsTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession
    .builder
    .appName("FireCallsSolvedQuestionsDatasetTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // in 10 days, there are 3 incidents
  val testData = Seq(
    FireCallInstance(Some(20110016), Some("T13"), Some(2003235), Some("Structure Fire"), Some("01/11/2002"), Some("01/10/2002"), Some("Other"), Some("01/11/2002 01:51:44 AM"), Some("2000 Block of CALIFORNIA ST"), Some("SF"), Some(94109), Some("B04"), Some("38"), Some("3362"), Some("3"), Some("3"), Some(3), Some(false), Some(""), Some(1), Some("TRUCK"), Some(2), Some("4"), Some("5"), Some("Pacific Heights"), Some("(37.7895840679362, -122.428071912459)"), Some("020110016-T13"), Some(2.95)),
    FireCallInstance(Some(20110022), Some("M17"), Some(2003241), Some("Medical Incident"), Some("01/16/2002"), Some("01/10/2002"), Some("Other"), Some("01/11/2002 03:01:18 AM"), Some("0 Block of SILVERVIEW DR"), Some("SF"), Some(94124), Some("B10"), Some("42"), Some("6495"), Some("3"), Some("3"), Some(3), Some(true), Some(""), Some(1), Some("MEDIC"), Some(1), Some("10"), Some("10"), Some("Bayview Hunters Point"), Some("(37.7337623673897, -122.396113802632)"), Some("020110022-M17"), Some(4.7)),
    FireCallInstance(Some(20110023), Some("M41"), Some(2003242), Some("Medical Incident"), Some("01/21/2002"), Some("01/10/2002"), Some("Other"), Some("01/11/2002 02:39:50 AM"), Some("MARKET ST/MCALLISTER ST"), Some("SF"), Some(94102), Some("B03"), Some("01"), Some("1455"), Some("3"), Some("3"), Some(3), Some(true), Some(""), Some(1), Some("MEDIC"), Some(2), Some("3"), Some("6"), Some("Tenderloin"), Some("(37.7811772186856, -122.411699931232)"), Some("020110023-M41"), Some(2.4333334))
  ).toDS()

  test("Number of calls logged in the last seven days") {
    val fireTsDS = testData
      .withColumnRenamed("Delay", "ResponseDelayedinMins")
      .withColumn("IncidentDate", to_timestamp($"CallDate", "MM/dd/yyyy"))
      .withColumn("OnWatchDate", to_timestamp($"WatchDate", "MM/dd/yyyy"))
      .withColumn("AvailableDtTS", to_timestamp($"AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
      .drop("CallDate", "WatchDate", "AvailableDtTm")
      .as[FireCallInstanceWithTimestamps]

    val maxDate = fireTsDS
      .select(max($"IncidentDate"))
      .take(1)(0)
      .getTimestamp(0)

    val lastSevenDaysDS = fireTsDS
      .filter($"IncidentDate" >= date_sub(lit(maxDate), 7))

    val callCount = lastSevenDaysDS.count()

    // last 7 should be 2
    assert(callCount == 2)
  }

  test("Most common types of fire calls") {
    val fireTsDS = testData
      .withColumnRenamed("Delay", "ResponseDelayedinMins")
      .withColumn("IncidentDate", to_timestamp($"CallDate", "MM/dd/yyyy"))
      .withColumn("OnWatchDate", to_timestamp($"WatchDate", "MM/dd/yyyy"))
      .withColumn("AvailableDtTS", to_timestamp($"AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
      .drop("CallDate", "WatchDate", "AvailableDtTm")
      .as[FireCallInstanceWithTimestamps]

    val mostCommonCallTypes = fireTsDS
      .filter(_.CallType.isDefined)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .collect()

    // in our mock data, there are 2 Medical Incidents
    assert(mostCommonCallTypes.head.getString(0) == "Medical Incident")
    assert(mostCommonCallTypes.head.getLong(1) == 2)
  }

  test("Min, Max, Avg calculations") {
    val fireTsDS = testData
      .withColumnRenamed("Delay", "ResponseDelayedinMins")
      .withColumn("IncidentDate", to_timestamp($"CallDate", "MM/dd/yyyy"))
      .withColumn("OnWatchDate", to_timestamp($"WatchDate", "MM/dd/yyyy"))
      .withColumn("AvailableDtTS", to_timestamp($"AvailableDtTm", "MM/dd/yyyy hh:mm:ss a"))
      .drop("CallDate", "WatchDate", "AvailableDtTm")
      .as[FireCallInstanceWithTimestamps]

    val calculations = fireTsDS
      .agg(
        F.sum("NumAlarms").alias("Total Alarms"),
        F.avg("ResponseDelayedinMins").as("Average Delay in Mins"),
        F.min("ResponseDelayedinMins").alias("Minimum Delay in Mins"),
        F.max("ResponseDelayedinMins").alias("Maximum Delay in Mins")
      )
      .collect()

    // every incident is 1 alarm in NumAlarms
    assert(calculations.head.getLong(0) == 3)
    // Delay average is approximately 3.361111133 = 2.95 + 4.7 + 2.4333334
    assert(math.abs(calculations.head.getDouble(1) - 3.3611111) < 0.0001)
    // min is 2.4333334
    assert(calculations.head.getDouble(2) == 2.4333334)
    // max is 4.7
    assert(calculations.head.getDouble(3) == 4.7)
  }
}
