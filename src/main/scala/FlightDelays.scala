package learningSpark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Let's analyze a Flight data with Spark SQL
 *
 *  Our data is called departuredelays.
 */
 object FlightDelays {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Flight Delays")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

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

    // here is a DDL schema
    // you can also use this, let's try it!
    val delaySchema = "date STRING, delay INT, " +
      "distance INT, origin STRING, destination STRING"

    val flightDelaysDF = spark
      .read
      .format("csv")
      .option("header", value = true)
      .schema(delaySchema)
      // You can uncomment this like to see what Spark thinks
      // .option("inferSchema", "true")
      .load(departureDelaysFilePath)

    /*
    // we can make a managed table with this code!
    df
      .write
      .saveAsTable("managed_us_delay_flights_tbl")
    // default makes a directory under project root:
    // /home/sezai/IdeaProjects/sparkTraining/spark-warehouse/
    */

    // we need a Table or View to query with SQL
    println("Making a temp view from df, named us_delay_flights_tbl\n")
    flightDelaysDF.createOrReplaceTempView("us_delay_flights_tbl")

    println("Running an spark.sql for longDistance flights: \n")
    // Now we can run these!
    val longDistanceSqlWay: DataFrame = spark
      .sql("""SELECT distance, origin, destination
              FROM us_delay_flights_tbl WHERE distance > 1000
              ORDER BY distance DESC""")
    // .show(10)

    println("Running an Dataframe API for longDistance flights : \n")
    // Now let's make the query with Dataframe API
    val longDistanceDF = flightDelaysDF
      .select($"distance", $"origin", $"destination")
      .where($"distance" > 1000)
      .orderBy($"distance".desc)

    println("The explaining FOR two dataframes are: \n")

    longDistanceSqlWay.explain()
    longDistanceDF.explain()

    println("\nThey are exactly the same!\n")

    // Let's try caching the flightDelaysDF
    // it is obviously, faster
    flightDelaysDF.cache()
    // we need to materialize the cache to be actuate.
    flightDelaysDF.count()

    val startTime = System.nanoTime()

    longDistanceDF.show(truncate = false)

    println("Let's see some stats about longDistanceDF\n")

    longDistanceDF
      .stat
      .freqItems(Seq("origin"))
      .show(truncate = false)

    val pdxCount = longDistanceDF
      .select("origin")
      .where($"origin" === "PDX")
      .count()

    println(s"\nOrigin being PDX, flight count is $pdxCount")

    val sttCount = longDistanceDF
      .select("origin")
      .where($"origin" === "STT")
      .count()

    println(s"Origin being STT flight count is $sttCount")

    // Next, we’ll find all flights between San Francisco (SFO) and Chicago
    // (ORD) with at least a two-hour delay:

    // with df
    val sfChiDelayed = flightDelaysDF
      .select($"date", $"delay", $"origin", $"destination")
      .where($"origin" === "SFO")
      .where($"destination" === "ORD")
      .where($"delay" >= 120)
      .orderBy($"delay".desc)
    // .count()
    // 56

    println("All flights between SFO and ORD with at least a two-hour delay\n")
    sfChiDelayed
      .show(truncate = false)

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

    // one way would be to make different Dataframes and concatenate them
    val delayCategorizedVeryLong = flightDelaysDF
      .select($"delay", $"origin", $"destination")
      .filter($"delay" > 360)
      .withColumn("Flight_Delays", lit("Very Long Delays"))
    // 1499
    // .count()

    val delayCategorizedLong = flightDelaysDF
      .select($"delay", $"origin", $"destination")
      .filter($"delay".between(120, 360))
      .withColumn("Flight_Delays", lit("Long Delays"))
    // 31983
    // .count()

    val delayCategorizedShort = flightDelaysDF
      .select($"delay", $"origin", $"destination")
      .filter($"delay".between(60, 120))
      .withColumn("Flight_Delays", lit("Short Delays"))

    val delayCategorizedTolerable = flightDelaysDF
      .select($"delay", $"origin", $"destination")
      .filter($"delay".between(0, 60))
      .withColumn("Flight_Delays", lit("Tolerable Delays"))

    val delayCategorizedNoDelay = flightDelaysDF
      .select($"delay", $"origin", $"destination")
      .filter($"delay" === 0)
      .withColumn("Flight_Delays", lit("No Delays"))

    val delayCategorizedEarly = flightDelaysDF
      .select($"delay", $"origin", $"destination")
      .filter($"delay" < 0)
      .withColumn("Flight_Delays", lit("Early"))

    // now that we calculated all of the Dataframes, we can union them
    val bigDelaysDf = delayCategorizedVeryLong
      .union(delayCategorizedLong)
      .union(delayCategorizedShort)
      .union(delayCategorizedTolerable)
      .union(delayCategorizedNoDelay)
      .union(delayCategorizedEarly)
      // order by multiple columns
      .orderBy($"origin", $"delay".desc)

    // Here is a simpler approach
    // instead of all that code!

    // Make the DataFrame transformations
    val dfWithFlightDelays = flightDelaysDF
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

    println("With column (Flight_Delays), with when - when - otherwise:\n")

    dfWithFlightDelays
      .show(truncate = false)

    println("A lot of Dataframes union:\n")
    // the result
    bigDelaysDf
      .show(truncate = false)

    // We can do the same with SQL
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

    // println(f"Duration without Caching : $timeDifference%.3f seconds")
    // 4.365 seconds
    println(f"Duration with Caching : $timeDifference%.3f seconds\n")
    // 3.713 seconds

  }
}
