package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// TODO
object FlightDelaysAdvanced {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("FlightDelaysAdvanced")
      .master("local[*]")
      .getOrCreate()

    // $ usage
    import spark.implicits._

    // verbose = False
    spark.sparkContext.setLogLevel("ERROR")

    // Define the data path as a val
    val departureDelaysFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/departuredelays.csv"
    }

    // Define the data path as a val
    val airportInfoFilepath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/airport-codes-na.txt"
    }

    // 1- Import two files and create two DataFrames, one
    // for airport (airports-na) information and one for
    // US flight delays (departureDelays).

    // we are intentionally making delay and distance as Strings!
    // we will learn to cast them later
    val departureDelaysSchema = StructType(
      Array(
        StructField("date", StringType, nullable = true),
        StructField("delay", StringType, nullable = true),
        StructField("distance", StringType, nullable = true),
        StructField("origin", StringType, nullable = true),
        StructField("destination", StringType, nullable = true)
      ))

    val airportInfoSchema = StructType(
      Array(
        StructField("City", StringType, nullable = true),
        StructField("State", StringType, nullable = true),
        StructField("Country", StringType, nullable = true),
        StructField("IATA", StringType, nullable = true),
      )
    )

    /*
    • The date column contains a string like 02190925.
    When converted, this maps to 02-19 09:25 am.
    • The delay column gives the delay in minutes between the scheduled and actual
    departure times. Early departures show negative numbers.
    • The distance column gives the distance in miles from the origin airport to the
    destination airport.
    • The origin column contains the origin IATA airport code.
    • The destination column contains the destination IATA airport code.
     */

    val departureDelaysDF = spark
      .read
      .format("csv")
      .option("header", value = true)
      .schema(departureDelaysSchema)
      .load(departureDelaysFilePath)

    // Here is a discovery about reading txt files
    // you read txt files as csv!
    val airportInfoDF = spark
      .read
      .format("csv")
      .option("header", value = true)
      .schema(airportInfoSchema)
      // delimiter is tabs
      .option("delimiter", "\t")
      .load(airportInfoFilepath)

    // departureDelaysDf.show(truncate = false)
    // airportInfoDf.show(truncate = false)

    println("departureDelaysDf made!")
    println("airportInfoDf made!")

    // Using expr(), convert the delay and distance columns from STRING to INT.

    // lets see the before after for it!
    departureDelaysDF.printSchema()

    // with DataFrame API
    departureDelaysDF
      // we can simply use withColumn
      // .withColumn("delay", expr("CAST(delay as INT) as delay"))
      // .withColumn("distance", expr("CAST(distance as INT) as distance"))
      // Or just cast them
      // This is more readable and maintainable.
      .withColumn("delay", $"delay".cast(IntegerType))
      .withColumn("distance", $"distance".cast(IntegerType))

    // now we have casted the datatypes
    departureDelaysDF.printSchema()
    // departureDelaysDf.show(truncate = false)

    // make views so we can do sql
    departureDelaysDF.createOrReplaceTempView("departureDelays")
    airportInfoDF.createOrReplaceTempView("airports_na")

    // Create temporary small table
    val fooDF = departureDelaysDF
      .filter(expr("""origin == 'SEA' AND destination == 'SFO' AND
              date like '01010%' AND delay > 0"""))
      .toDF()

    println("A small DF which is made from departureDelaysDf!")
    fooDF.createOrReplaceTempView("foo")

    // let see those tables
    println("let see those tables\n")
    println("Foo")
    spark.sql("SELECT * FROM foo LIMIT 10").show()

    println("airports_na")
    spark.sql("SELECT * FROM airports_na LIMIT 10").show()

    println("departureDelays")
    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

    // Union two tables
    // because fooDF is a small part of departureDelaysDF
    // the result will be simply equilavent to departureDelaysDF
    val bar = departureDelaysDF.union(fooDF)

    // make a table for it too
    bar.createOrReplaceTempView("bar")

    println("We ran departureDelaysDf.union(fooDF)\n")

    println("Filtering the result is made with 3 different approaches\n")
    println("expr(), Dataframe API, spark sql")
    println("with Dataframe API - union result DF\n")

    println("After all the filtering, we will see a duplication of the foo data")

    // Apache Spark expr() function to use SQL syntax
    // anywhere a column would be specified.
    bar
      .filter(expr("""origin == 'SEA' AND destination == 'SFO'
          AND date LIKE '01010%' AND delay > 0"""))
    //.show()

    // Without expr
    println("without expr - union result DF")
    println("After all the filtering, we will see a duplication of the foo data")
    bar
      .filter($"origin" === "SEA" &&
        $"destination" === "SFO" &&
        $"date".startsWith("01010") &&
        $"delay" > 0)
    //.show()

    // The bar DataFrame is the union of foo with delays.
    // Using the same filtering criteria
    // results in the bar DataFrame, we see a
    // duplication of the foo data, as expected:
    println("with spark SQL - union result DF")
    println("After all the filtering, we will see a duplication of the foo data")
    spark.sql("""SELECT *
          FROM bar
          WHERE origin = 'SEA'
            AND destination = 'SFO'
            AND date LIKE '01010%'
            AND delay > 0
            """)
    // .show()

    /*
    A common DataFrame operation is to join two DataFrames (or tables) together.
    By default, a Spark SQL join is an inner join, with the
    options being inner, cross, outer, full, full_outer, left, left_outer,
    right, right_outer, left_semi, and left_anti.
    */

    // Inner join between the airportInfoDf and foo
    println("Let's discover about joins!")

    println("Inner join between the airportInfoDf and foo - Book approach")
    fooDF.join(
      airportInfoDF.as("air"),
      $"air.IATA" === $"origin"
    ).select("City", "State", "date", "delay", "distance", "destination")
    //.show()

    // with SQL
    println("Inner join between the airportInfoDf and foo - with SQL")
    spark.sql("""SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
        FROM foo f
        JOIN airports_na a
        ON a.IATA = f.origin
        """)
    // .show()

    // Dataframe API
    println("Inner join between the airportInfoDf and foo - with dataframe API\n")
    fooDF
      .join(
        // IATA is from airportInfoDf - origin from foo
        airportInfoDF, $"IATA" === $"origin"
      )
      .select($"City", $"State", $"date", $"delay", $"distance", $"destination")
      .show()

    // make a table for delays, with SQL
    /* IN raw sql
    DROP TABLE IF EXISTS departureDelaysWindow;
    CREATE TABLE departureDelaysWindow AS
    SELECT origin, destination, SUM(delay) AS TotalDelays
    FROM departureDelays
    WHERE origin IN ('SEA', 'SFO', 'JFK')
    AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
    GROUP BY origin, destination;

    SELECT * FROM departureDelaysWindow
     */

    // make a window in Dataframe API
    println("\n Now lets learn about Windowing!")

    val departureDelaysWindowDf = departureDelaysDF
      .select($"origin", $"destination", $"delay")
      .filter($"origin".isin("SEA", "SFO", "JFK") &&
        $"destination".isin("SEA", "SFO", "JFK", "DEN", "ORD", "LAX", "ATL"))
      .groupBy("origin", "destination")
      .agg(sum("delay").alias("TotalDelays"))

    // make A table for it
    departureDelaysWindowDf.createOrReplaceTempView("departureDelaysWindow")

    println("departureDelaysWindowDf made!")
    //departureDelaysWindowDf.show()

    println("Dense Rank calculation - with spark SQL:\n")
    spark.sql("""
       SELECT origin, destination, TotalDelays, rank
        FROM (
          SELECT origin, destination, TotalDelays, dense_rank()
            OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
            FROM departureDelaysWindow
        ) t
       WHERE rank <= 3
       """)
    // .show()

    // Now with Dataframe API
    println("Now with Dataframe API - dense Rank\n")
    // Define the window specification
    val windowSpec = Window
      .partitionBy("origin")
      .orderBy($"TotalDelays".desc)

    departureDelaysWindowDf
      .select($"origin", $"destination", $"TotalDelays")
      .withColumn("rank", dense_rank().over(windowSpec))
      .where($"rank" <= 3)
      .show()

    // new col to foo with expr()
    println("new col to foo with expr()")
    val fooSecondDF = fooDF
      .withColumn("status",
        expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
      )

    // fooSecondDF.show()

    // with Dataframe API
    println("new col to foo with Dataframe API - status col\n")
    val fooThirdDF = fooDF
      .withColumn("status",
        when($"delay" <= 10, "On-Time")
          .otherwise("Delayed")
      )

    fooThirdDF.show()

    // Returns a new Dataset with column dropped.
    val fooFourthDF = fooSecondDF.drop($"delay")
    println("delay dropped from foo2\n")
    fooFourthDF.show()

    // rename some column
    println("withColumnRenamed example, status is now flight_status \n:")
    val fooFifthDF = fooSecondDF.withColumnRenamed("status", "flight_status")
    fooFifthDF.show()

    println("Pivoting example\n")
    /*
    SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
    FROM departureDelays
      WHERE origin = 'SEA'
     */

    val filteredDf = departureDelaysDF
      .where($"origin" === "SEA")
      // substr(0, 2) extracts the first two characters
      // from the date string.
      .withColumn("month", $"date".substr(0, 2).cast(IntegerType))
      .select("destination", "month", "delay")

    filteredDf.show()

    println("Here is all the distinct values of month:\n")

    filteredDf
      .select($"month")
      .distinct()
      .orderBy($"month")
      .show(truncate = false)

    println("Pivotted DF - TO JAN - FEB - MAR\n")

    val pivottedPivotDf = filteredDf
      .groupBy("destination")
      // Pivot on months January and February
      .pivot("month", Seq(1, 2, 3))
      // this became all NULL because there is no JAN, FEB in filteredDf
      //.pivot("month", Seq("JAN", "FEB"))
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

    pivottedPivotDf.show()

  }
}