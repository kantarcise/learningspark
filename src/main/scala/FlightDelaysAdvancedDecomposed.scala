package learningSpark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Now we are ready for some decomposition!
object FlightDelaysAdvancedDecomposed {

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    val departureDelaysFilePath = getFilePath("departuredelays.csv")
    val airportInfoFilePath = getFilePath("airport-codes-na.txt")

    val departureDelaysDF = readDepartureDelays(spark, departureDelaysFilePath)
    val airportInfoDF = readAirportInfo(spark, airportInfoFilePath)

    println("departureDelaysDf made!")
    println("airportInfoDf made!")

    castTypesofDepartureDelays(departureDelaysDF)

    performInitialDataProcessing(departureDelaysDF)

    val fooDF = createSmallDataFrame(departureDelaysDF)

    // make all the tables
    makeTables(spark, fooDF, departureDelaysDF, airportInfoDF)

    unionTables(departureDelaysDF, fooDF)

    joinDataFrames(fooDF, airportInfoDF)

    val departureDelaysWindowDf = performWindowing(departureDelaysDF)

    // make A table for it
    departureDelaysWindowDf.createOrReplaceTempView("departureDelaysWindow")
    println("departureDelaysWindowDf made!")

    calculateDenseRank(spark, departureDelaysWindowDf)

    println("fooDF before addStatusColumn\n")
    fooDF.show()
    addStatusColumn(fooDF)

    println("fooDF after addStatusColumn\n")
    fooDF.show()

    dropDelayColumn(fooDF)
    renameStatusColumn(fooDF)

    println("fooDF after delay dropped and status renamed\n")
    fooDF.show()

    performPivoting(departureDelaysDF)
  }

  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder
      .appName("FlightDelaysAdvanced")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def getFilePath(fileName: String): String = {
    val projectDir = System.getProperty("user.dir")
    s"$projectDir/data/$fileName"
  }

  def readDepartureDelays(spark: SparkSession, filePath: String): DataFrame = {

    val departureDelaysSchema = StructType(Array(
      StructField("date", StringType, nullable = true),
      StructField("delay", StringType, nullable = true),
      StructField("distance", StringType, nullable = true),
      StructField("origin", StringType, nullable = true),
      StructField("destination", StringType, nullable = true)
    ))

    spark.read
      .format("csv")
      .option("header", value = true)
      .schema(departureDelaysSchema)
      .load(filePath)

  }

  def readAirportInfo(spark: SparkSession, filePath: String): DataFrame = {
    val airportInfoSchema = StructType(Array(
      StructField("City", StringType, nullable = true),
      StructField("State", StringType, nullable = true),
      StructField("Country", StringType, nullable = true),
      StructField("IATA", StringType, nullable = true)
    ))

    spark.read
      .format("csv")
      .option("header", value = true)
      .schema(airportInfoSchema)
      .option("delimiter", "\t")
      .load(filePath)
  }

  def castTypesofDepartureDelays(departureDelaysDF: DataFrame): Unit = {
    import departureDelaysDF.sparkSession.implicits._

    // let's print schemas before and after to see the effect!

    departureDelaysDF.printSchema()

    departureDelaysDF
      .withColumn("delay", $"delay".cast(IntegerType))
      .withColumn("distance", $"distance".cast(IntegerType))

    departureDelaysDF.printSchema()

  }

  def performInitialDataProcessing(departureDelaysDF: DataFrame): Unit = {
    departureDelaysDF.createOrReplaceTempView("departureDelays")
  }

  def  createSmallDataFrame(departureDelaysDF: DataFrame): DataFrame = {

    println("A small DF which is made from departureDelaysDf!\n")
    val fooDF = departureDelaysDF
      .filter(expr(
        """origin == 'SEA' AND destination == 'SFO' AND
         date like '01010%' AND delay > 0"""))
      .toDF()

    fooDF
  }

  def makeTables(spark: SparkSession,
                 fooDF: DataFrame,
                 departureDelaysDF: DataFrame,
                 airportInfoDF: DataFrame): Unit = {

    fooDF.createOrReplaceTempView("foo")
    departureDelaysDF.createOrReplaceTempView("departureDelays")
    airportInfoDF.createOrReplaceTempView("airports_na")

    println("let see those tables\n")
    println("Foo")
    spark.sql("SELECT * FROM foo LIMIT 10").show()

    println("airports_na")
    spark.sql("SELECT * FROM airports_na LIMIT 10").show()

    println("departureDelays")
    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
  }

  def unionTables(departureDelaysDF: DataFrame, fooDF: DataFrame): Unit = {

    // Union two tables
    // because fooDF is a small part of departureDelaysDF
    // the result will be simply equilavent to departureDelaysDF
    import departureDelaysDF.sparkSession.implicits._

    val bar = departureDelaysDF.union(fooDF)
    // make a table for it too
    bar.createOrReplaceTempView("bar")

    println("We ran departureDelaysDf.union(fooDF)\n")

    println("Filtering the result is made with 3 different approaches\n")
    println("expr(), Dataframe API, spark sql")
    println("with Dataframe API - union result DF\n")

    bar.filter(expr(
      """origin == 'SEA' AND destination == 'SFO' AND
         date LIKE '01010%' AND delay > 0"""))
    // .show()

    // Without expr
    println("without expr - union result DF")
    println("After all the filtering, we will see a duplication of the foo data")
    bar.filter($"origin" === "SEA" &&
      $"destination" === "SFO" &&
      $"date".startsWith("01010") &&
      $"delay" > 0)
    // .show()

    bar.sqlContext.sql(
      """SELECT *
         FROM bar
         WHERE origin = 'SEA'
           AND destination = 'SFO'
           AND date LIKE '01010%'
           AND delay > 0
      """)
    //.show()
  }

  def joinDataFrames(fooDF: DataFrame, airportInfoDF: DataFrame): Unit = {

    import fooDF.sparkSession.implicits._

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
      )
      .select("City", "State", "date", "delay", "distance", "destination")
    // .show()

    // Dataframe API
    println("Inner join between the airportInfoDf and foo - with dataframe API\n")
    fooDF.join(
        airportInfoDF, $"IATA" === $"origin")
      .select($"City", $"State", $"date", $"delay", $"distance", $"destination")
      .show()

    // with SQL
    println("Inner join between the airportInfoDf and foo - with SQL")
    fooDF.sqlContext.sql(
      """SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
         FROM foo f
         JOIN airports_na a
         ON a.IATA = f.origin
      """)
    // .show()
  }

  def performWindowing(departureDelaysDF: DataFrame): DataFrame = {

    import departureDelaysDF.sparkSession.implicits._

    // make a window in Dataframe API
    println("\n Now lets learn about Windowing!")
    departureDelaysDF
      .select($"origin", $"destination", $"delay")
      .filter($"origin".isin("SEA", "SFO", "JFK") &&
        $"destination".isin("SEA", "SFO", "JFK", "DEN", "ORD", "LAX", "ATL"))
      .groupBy("origin", "destination")
      .agg(sum("delay").alias("TotalDelays"))
  }

  def calculateDenseRank(spark: SparkSession, departureDelaysWindowDf: DataFrame): Unit = {

    import departureDelaysWindowDf.sparkSession.implicits._

    println("Dense Rank calculation - with spark SQL:\n")
    spark.sql(
      """SELECT origin, destination, TotalDelays, rank
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
    val windowSpec = Window
      .partitionBy("origin")
      .orderBy($"TotalDelays".desc)

    departureDelaysWindowDf
      .select($"origin", $"destination", $"TotalDelays")
      .withColumn("rank", dense_rank().over(windowSpec))
      .where($"rank" <= 3)
      .show()
  }

  def addStatusColumn(fooDF: DataFrame): DataFrame = {
    import fooDF.sparkSession.implicits._

    /*
    println("new col to foo with expr()")
    fooDF
      .withColumn("status",
        expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
      )
    */

    // with Dataframe API
    fooDF.withColumn("status",
      when($"delay" <= 10, "On-Time")
        .otherwise("Delayed"))
  }

  def dropDelayColumn(fooDF: DataFrame): DataFrame = {
    import fooDF.sparkSession.implicits._
    fooDF.drop($"delay")
  }

  def renameStatusColumn(fooDF: DataFrame): DataFrame = {
    fooDF.withColumnRenamed("status", "flight_status")
  }

  def performPivoting(departureDelaysDF: DataFrame): Unit = {
    import departureDelaysDF.sparkSession.implicits._

    println("Pivoting example\n")

    // In SQL
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