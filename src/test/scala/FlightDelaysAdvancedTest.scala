package learningSpark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class FlightDelaysAdvancedTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("FlightDelaysAdvancedTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val departureDelaysData = Seq(
    ("02190925", 10, 500, "SEA", "SFO"),
    ("02190926", -5, 600, "JFK", "LAX"),
    ("02190927", 20, 700, "ATL", "ORD")
  )

  val airportInfoData = Seq(
    ("Seattle", "WA", "USA", "SEA"),
    ("San Francisco", "CA", "USA", "SFO"),
    ("New York", "NY", "USA", "JFK")
  )

  val departureDelaysDF: DataFrame = spark
    .createDataFrame(departureDelaysData)
    .toDF("date", "delay", "distance", "origin", "destination")

  val airportInfoDF: DataFrame = spark
    .createDataFrame(airportInfoData)
    .toDF("City", "State", "Country", "IATA")

  test("DataFrame creation and schema validation") {
    assert(departureDelaysDF.columns.length == 5)
    assert(departureDelaysDF.schema.fields.map(_.dataType) sameElements Array(StringType, IntegerType, IntegerType, StringType, StringType))

    assert(airportInfoDF.columns.length == 4)
    assert(airportInfoDF.schema.fields.map(_.dataType) sameElements Array(StringType, StringType, StringType, StringType))

    assert(departureDelaysDF.count() == 3)
    assert(airportInfoDF.count() == 3)
  }

  test("Union operation") {
    val fooDF = departureDelaysDF
      .filter($"origin" === "SEA"
        && $"destination" === "SFO"
        && $"date".startsWith("021909")
        && $"delay" > 0)

    val barDF = departureDelaysDF
      .union(fooDF)

    // there is only one instance at fooDF
    assert(fooDF.count() == 1)
    // the union is simply total of them
    assert(barDF.count() == 4)
  }

  test("Join operation") {
    val fooDF = departureDelaysDF
      .filter($"origin" === "SEA" &&
        $"destination" === "SFO" &&
        $"date".startsWith("021909")
        && $"delay" > 0)

    // ONly one instance that is eligible to join
    // From fooDF                          -  From airportInfoDF
    // ("02190925", 10, 500, "SEA", "SFO") - ("Seattle", "WA", "USA", "SEA")
    val joinedDF = fooDF
      .join(airportInfoDF, $"origin" === $"IATA")
      .select("City", "State", "date", "delay", "distance", "destination")

    // expected result
    val expectedData = Seq(
      ("Seattle", "WA", "02190925", 10, 500, "SFO")
    )

    val expectedDF = spark
      .createDataFrame(expectedData)
      .toDF("City", "State", "date", "delay", "distance", "destination")

    assert(expectedDF.collect() === joinedDF.collect())

    // another way to check equality can be these
    assert(joinedDF.except(expectedDF).count() == 0)
    assert(expectedDF.except(joinedDF).count() == 0)
  }

  test("Windowing and ranking") {
    val windowSpec = Window
      .partitionBy("origin")
      .orderBy(col("TotalDelays").desc)

    val departureDelaysWindowDF = departureDelaysDF
      .filter($"origin".isin("SEA", "SFO", "JFK")
        && $"destination".isin("SEA", "SFO", "JFK", "DEN", "ORD", "LAX", "ATL")
      )
      .groupBy("origin", "destination")
      .agg(sum("delay").alias("TotalDelays"))

    val rankedDF = departureDelaysWindowDF
      .select($"origin", $"destination", $"TotalDelays")
      .withColumn("rank", dense_rank().over(windowSpec))
      .where($"rank" <= 3)

    val expectedData = Seq(
      ("SEA", "SFO", 10, 1),
      ("JFK", "LAX", -5, 1)
    )

    val expectedDF = spark.createDataFrame(expectedData)
      .toDF("origin", "destination", "TotalDelays", "rank")

    // make a set so that order does not matter
    assert(rankedDF.collect().toSet === expectedDF.collect().toSet)
    // also viable
    assert(rankedDF.except(expectedDF).count() == 0)
    assert(expectedDF.except(rankedDF).count() == 0)
  }

  test("Pivoting") {
    val filteredDF = departureDelaysDF
      .where($"origin" === "SEA")
      .withColumn("month", $"date".substr(0, 2).cast(IntegerType))
      .select("destination", "month", "delay")

    val pivottedPivotDF = filteredDF
      .groupBy("destination")
      .pivot("month", Seq(1, 2, 3))
      .agg(
        round(avg($"delay"), 2).as("AvgDelay"),
        max($"delay").as("MaxDelay")
      )
      .orderBy($"destination")
      .withColumnRenamed("1_AvgDelay", "JAN_AvgDelay")
      .withColumnRenamed("1_MaxDelay", "JAN_MaxDelay")
      .withColumnRenamed("2_AvgDelay", "FEB_AvgDelay")
      .withColumnRenamed("2_MaxDelay", "FEB_MaxDelay")
      .withColumnRenamed("3_AvgDelay", "MAR_AvgDelay")
      .withColumnRenamed("3_MaxDelay", "MAR_MaxDelay")

    // you can see the DF if you want to
    // pivottedPivotDF.show()

    // for our filteredDF, only one instance with SEA destination
    // that happens in February, which has 10.0 avg delay and 10 max delay
    assert(pivottedPivotDF.select($"FEB_AvgDelay").collect().take(1)(0).getDouble(0) === 10.0)
    assert(pivottedPivotDF.select($"FEB_MaxDelay").collect().take(1)(0).getInt(0) === 10)

    // some basic assertions
    assert(pivottedPivotDF.columns.contains("JAN_AvgDelay"))
    assert(pivottedPivotDF.columns.contains("FEB_AvgDelay"))
    assert(pivottedPivotDF.columns.contains("MAR_AvgDelay"))
  }
}