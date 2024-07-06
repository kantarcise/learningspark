package learningSpark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Just the Dataset Version
object FlightDelaysDataset {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("FlightDelaysDataset")
      .master("local[*]")
      .getOrCreate()

    // verbose = False
    spark.sparkContext.setLogLevel("ERROR")

    // Import implicits for encoders
    import spark.implicits._

    // Define the data path as a val
    val departureDelaysFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/departuredelays.csv"
    }

    // Here is our schema
    val departureDelaysSchema = StructType(
      Array(
        StructField("date", StringType, nullable = true),
        StructField("delay", IntegerType, nullable = true),
        StructField("distance", IntegerType, nullable = true),
        StructField("origin", StringType, nullable = true),
        StructField("destination", StringType, nullable = true)
      )
    )

    val flightDelaysDS = spark
      .read
      .format("csv")
      .option("header", value = true)
      .schema(departureDelaysSchema)
      .load(departureDelaysFilePath)
      .as[Flights]

    // we need a Table or View to query with SQL
    println("Making a temp view from ds, named us_delay_flights_tbl\n")
    flightDelaysDS.createOrReplaceTempView("us_delay_flights_tbl")

    println("Running an spark.sql for longDistance flights: \n")
    // Now we can run these!
    val longDistanceSqlWay: DataFrame = spark.sql("""SELECT distance, origin, destination
              FROM us_delay_flights_tbl WHERE distance > 1000
              ORDER BY distance DESC""")
    // .show(10)

    println("Running an Dataset API for longDistance flights : \n")
    // now let's make it with Dataset API
    val longDistanceDS = flightDelaysDS
      .select($"distance", $"origin", $"destination")
      .where($"distance" > 1000)
      .orderBy($"distance".desc)

    println("The explaining FOR \n")

    longDistanceSqlWay.explain()
    longDistanceDS.explain()

    println("They are exactly the same!\n")

    // Let's try caching the flightDelaysDS
    // it is obviously, faster
    flightDelaysDS.cache()
    // we need to materialize the cache to be actuate.
    flightDelaysDS.count()

    val startTime = System.nanoTime()

    longDistanceDS.show(truncate = false)

    println("Let's see some stats about longDistanceDS\n")

    longDistanceDS
      .stat
      .freqItems(Seq("origin"))
      .show(truncate = false)

    val pdxCount = longDistanceDS
      .filter($"origin" === "PDX")
      .count()

    println(s"Origin being PDX flight count is $pdxCount")

    val sttCount = longDistanceDS
      .filter($"origin" === "STT")
      .count()

    println(s"Origin being STT flight count is $sttCount")

    // Next, we’ll find all flights between San Francisco (SFO) and Chicago
    // (ORD) with at least a two-hour delay:

    // with ds
    val sfChiDelayed = flightDelaysDS
      .filter($"origin" === "SFO")
      .filter($"destination" === "ORD")
      .filter($"delay" >= 120)
      .select($"date", $"delay", $"origin", $"destination")
      .orderBy($"delay".desc)
    // .count()
    // 56

    sfChiDelayed.show(truncate = false)

    println("Running spark.sql for sfChiDelayed\n")
    // now with SQL
    spark.sql("""SELECT date, delay, origin, destination
              FROM us_delay_flights_tbl
              WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
              ORDER by delay DESC""").show(10)

    // In the following example, we want to label all US
    // flights, regardless of origin and destination,
    // with an indication of the delays they experienced:
    //  - Very Long Delays (> 6 hours),
    //  - Long Delays (2–6 hours), etc.
    //  We’ll add these human-readable labels in a new column called Flight_Delays:

    println("With column (Flight_Delays), when something, do this when other thing do that\n")

    // Create the Dataset transformations
    val dsWithFlightDelays = flightDelaysDS
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

    println("Here is a simpler approach, instead of all that code!\n")
    dsWithFlightDelays
      .show(truncate = false)

    println("A lot of Datasets union:\n")
    // the result
    dsWithFlightDelays
      .show(truncate = false)

    // Now, with SQL
    println("Now, with spark.sql\n")
    spark.sql("""SELECT delay, origin, destination,
          CASE
          WHEN delay > 360 THEN 'Very Long Delays'
          WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
          WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
          WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
          WHEN delay = 0 THEN 'No Delays'
          ELSE 'Early'
          END AS Flight_Delays
          FROM us_delay_flights_tbl
          ORDER BY origin, delay DESC""").show(10)

    println("Here are all the catalog stuff:\n")
    println(spark.catalog.listDatabases())
    println(spark.catalog.listTables())
    println(spark.catalog.listColumns("us_delay_flights_tbl"))

    val endTime = System.nanoTime()

    val timeDifference = (endTime - startTime)  / 1e9d

    println(f"Duration with Caching : $timeDifference%.3f seconds\n")

  }
}
