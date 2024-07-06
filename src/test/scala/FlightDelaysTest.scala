package learningSpark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

class FlightDelaysTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession
    .builder
    .appName("Flight Delays Test")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Import implicits for encoders
  import spark.implicits._

  // Define sample data
  val sampleData: DataFrame = Seq(
    ("01220625", 333, 602, "ABE", "ATL"),
    ("01220607", 219, 569, "ABE", "ORD"),
    ("02011325", 0, 1511, "PDX", "ORD"),
    ("02011535", 14, 1404, "PDX", "DFW"),
    ("01220955", -6, 349, "PIT", "MDW"),
    ("01221050", 0, 1576, "PIT", "PHX"),
    ("01221705", 30, 401, "PIT", "BNA"),
    ("03021810", -2, 1604, "SFO", "ORD"),
    ("03020925", 3, 1604, "SFO", "ORD"),
    ("03022330", 33, 1604, "SFO", "ORD"),
    ("03032330", 107, 1604, "SFO", "ORD"),
    ("02281405", 189, 1410, "STT", "JFK"),
    ("02281440", -5, 962, "STT", "MIA"),
    ("02280935", -10, 962, "STT", "MIA"),
    ("02281715", 263, 962, "STT", "MIA"),
    ("02011440", 12, 59, "STT", "SJU")
  ).toDF("date", "delay", "distance", "origin", "destination")

  // Create or replace the temp view once for SQL queries
  sampleData.createOrReplaceTempView("us_delay_flights_tbl")

  test("Test long distance flights using DataFrame API") {
    // Test long distance flights using DataFrame API
    val longDistanceDF = sampleData
      .select($"distance", $"origin", $"destination")
      .where($"distance" > 1000)
      .orderBy($"distance".desc)

    assert(longDistanceDF.count() == 8)
    assert(longDistanceDF.filter($"origin" === "PDX").count() == 2)
    assert(longDistanceDF.filter($"origin" === "SFO").count() == 4)

    // Test long distance flights using SQL
    val longDistanceSqlWay = spark.sql(
      """SELECT distance, origin, destination
        |FROM us_delay_flights_tbl
        |WHERE distance > 1000
        |ORDER BY distance DESC
      """.stripMargin)

    assert(longDistanceSqlWay.count() == 8)
    assert(longDistanceSqlWay.filter($"origin" === "PDX").count() == 2)
    assert(longDistanceSqlWay.filter($"origin" === "SFO").count() == 4)
  }

  test("Test flight delays categorization") {
    // Make the DataFrame transformations
    val dfWithFlightDelays = sampleData
      .select($"delay", $"origin", $"destination")
      .withColumn("Flight_Delays",
        when($"delay" > 360, "Very Long Delays")
          .when($"delay" > 120 && $"delay" < 360, "Long Delays")
          .when($"delay" > 60 && $"delay" < 120, "Short Delays")
          .when($"delay" > 0 && $"delay" < 60, "Tolerable Delays")
          .when($"delay" === 0, "No Delays")
          .otherwise("Early")
      )
      .orderBy($"origin", $"delay".desc)

    val flightDelaysCategorized = dfWithFlightDelays
      .groupBy("Flight_Delays")
      .count()

    assert(flightDelaysCategorized.filter($"Flight_Delays" === "Long Delays").head().getLong(1) == 4)
    assert(flightDelaysCategorized.filter($"Flight_Delays" === "Short Delays").head().getLong(1) == 1)
    assert(flightDelaysCategorized.filter($"Flight_Delays" === "Tolerable Delays").head().getLong(1) == 5)
    assert(flightDelaysCategorized.filter($"Flight_Delays" === "No Delays").head().getLong(1) == 2)
    assert(flightDelaysCategorized.filter($"Flight_Delays" === "Early").head().getLong(1) == 4)
  }

}
