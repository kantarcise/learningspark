package learningSpark

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

/**
 * Let's now answer the questions we had, with
 * typed transformations!
 */
object FlightDelaysDataset {

  // we need some case classes for intermediate processes
  case class DistanceBetween(distance: Int,
                             origin: String,
                             destination: String)

  case class FlightWithoutDistance(date: String,
                                   delay: Int,
                                   origin: String,
                                   destination: String)

  case class FlightDelayWithCategory(delay: Int,
                                     origin: String,
                                     destination: String,
                                     Flight_Delays: String)

  /**
   * We can replicate freqItems ourselves!
   *
   * The method freqItems has some untyped transformations inside,
   * if this is an issue, we can use an Aggregator and have
   * typed transformations too!
   */
  object FrequentItems {

    case class FrequentItem(SomeFrequentItems: Seq[String])

    /** For a given dataset, and colName
     * calculate most n frequent elements inside!
     */
    def freqItems[T](ds: Dataset[T], colName: String,
                     topN: Int = 10): Dataset[FrequentItem] = {
      import ds.sparkSession.implicits._

      // Extract the column values as a Dataset of Strings
      val colDS = ds
        .map(row => row.getClass.getMethod(colName).invoke(row).asInstanceOf[String])

      // Count the occurrences of each item
      val itemCounts = colDS.groupBy("value").count()

      // Filter to get the most frequent items
      val frequentItems = itemCounts
        .orderBy($"count".desc).limit(topN).select("value").as[String].collect()

      // Wrap the result in a case class and convert to a Dataset
      val result = Seq(FrequentItem(frequentItems))
      ds.sparkSession.createDataset(result)
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Flight Delays Dataset")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val departureDelaysFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/departuredelays.csv"
    }

    val flightDelaysDS = spark
      .read
      .format("csv")
      .option("header", value = true)
      .schema(implicitly[Encoder[Flights]].schema)
      .load(departureDelaysFilePath)
      .as[Flights]

    // we need a Table or View to query with SQL
    println("Making a temp view from ds, named us_delay_flights_tbl\n")
    flightDelaysDS.createOrReplaceTempView("us_delay_flights_tbl")

    println("Running an Dataset API for longDistance flights : \n")

    val longDistanceDS = flightDelaysDS
      .filter(_.distance > 1000)
      .map(value => DistanceBetween(value.distance,
        value.origin, value.destination))
      .orderBy('distance.desc)
      // or
      // .orderBy($"distance".desc)

    println("Let's run .explain():\n")
    longDistanceDS.explain()

    // Let's try caching the flightDelaysDS
    // it is obviously, faster
    flightDelaysDS.cache()
    // we need to materialize the cache to be actuate.
    flightDelaysDS.count()

    val startTime = System.nanoTime()

    longDistanceDS.show(truncate = false)

    println("Let's see some stats about longDistanceDS\n")

    println("Top 30 Origins in our data:\n")

    // Apply the custom freqItems method
    val frequentOriginsDS = FrequentItems.freqItems(
      longDistanceDS, "origin", topN = 30)

    // Show the result
    frequentOriginsDS.show(truncate = false)

    val pdxCount = longDistanceDS
      .filter(_.origin.contains("PDX"))
      .count()

    println(s"Origin being PDX flight count is $pdxCount")

    val sttCount = longDistanceDS
      .filter(_.origin.contains("STT"))
      .count()

    println(s"Origin being STT flight count is $sttCount")

    // Next, we’ll find all flights between San Francisco (SFO) and Chicago
    // (ORD) with at least a two-hour delay:

    // with typed transformations
    val sfChiDelayed = flightDelaysDS
      .filter(flight => flight.origin == "SFO" && flight.destination == "ORD" && flight.delay >= 120)
      .map(flight => FlightWithoutDistance(flight.date, flight.delay, flight.origin, flight.destination))
      .orderBy($"delay".desc)
    // .count()
    // 56

    println("All flights between SFO and ORD with at least a two-hour delay\n")
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

    // Make the typed transformations

    // Apply typed transformations
    val dsWithFlightDelays = flightDelaysDS
      .map(flight => FlightDelayWithCategory(
        flight.delay,
        flight.origin,
        flight.destination,
        categorizeDelay(flight.delay)
      ))
      .orderBy($"origin", $"delay".desc)

    println("Here is delay categorized, with typed transformations!\n")
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

  /** Define a function to categorize delays
   *
   * @param delay: The amount of delay
   * @return The category
   */
  def categorizeDelay(delay: Int): String = {
    if (delay > 360) "Very Long Delays"
    else if (delay > 120 && delay <= 360) "Long Delays"
    else if (delay > 60 && delay <= 120) "Short Delays"
    else if (delay > 0 && delay <= 60) "Tolerable Delays"
    else if (delay == 0) "No Delays"
    else "Early"
  }
}
