package learningSpark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Let's now answer some questions that the book gave us as exercise!
 *
 * We have 7 questions to answer.
 *
 * Exercise keep us alive :)
 */
object FireCallsTrainingQuestions {

  // the same schema
  val fireSchema: StructType = StructType(Array(
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
    StructField("Delay", FloatType, nullable = true))
  )

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val fireCallsPath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-fire-calls.csv"
    }

    val fireDF: DataFrame = spark
      .read
      .format("csv")
      .option("header", value = true)
      .schema(fireSchema)
      .load(fireCallsPath)

    // for to_timestamp, we can define patterns as
    val simpleTimePattern = "MM/dd/yyyy"
    val complexTimePattern = "MM/dd/yyyy hh:mm:ss a"

    // before start, get to the timestamps
    val fireTsDF = fireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), simpleTimePattern))
      .drop($"CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), simpleTimePattern))
      .drop($"WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), complexTimePattern))
      .drop($"AvailableDtTm")

    // we will repeatedly use fireTsDF
    fireTsDF.cache()

    println("\nDF with timestamps\n")
    fireTsDF.show()

    // To evaluate some stats on timestamps, we can generate
    // some Timestamps ourselves
    val endOf2018TS = to_timestamp(lit("12/31/2018"), simpleTimePattern)
    val beginningOf2018TS = to_timestamp(lit("01/01/2018"), simpleTimePattern)

    // 1 • What were all the different types of fire calls in 2018?
    val distinctTypesOfFireCallsDF = fireTsDF
      .where(col("IncidentDate") >= beginningOf2018TS)
      .where(col("IncidentDate") <= endOf2018TS)
      .select(col("CallType"))
      .distinct()
      // or we can also use
      // .dropDuplicates()

    // we can also use, between() !
    val testingBetweenDF = fireTsDF
      .where($"IncidentDate".between(beginningOf2018TS, endOf2018TS))
      .select($"CallType")
      .distinct()

    // in 2018
    println("What were all the different types of fire calls in 2018?\n")
    distinctTypesOfFireCallsDF
      .show(100)

    println("distinctTypesOfFireCalls in 2018, with between()\n")
    testingBetweenDF
      .show(100)

    // To check they are the same in count:
    assert(distinctTypesOfFireCallsDF.count() == testingBetweenDF.count())

    // If you want to see all distinct CallTypes
    //    fireTsDF
    //      .select("CallType")
    //      .distinct()
    //      .show(100, truncate = false)

    // 2 • What months within the year 2018 saw the highest number of fire calls?

    val fireCallsByMonthIn2018DF = fireTsDF
      .where(col("IncidentDate") >= beginningOf2018TS)
      .where(col("IncidentDate") <= endOf2018TS)
      .groupBy(month(col("IncidentDate")).alias("Month"))
      // count all of them which is grouped by months
      .agg(count("*").alias("CallCount"))
      // .orderBy(col("CallCount"))
      .orderBy(desc("CallCount"))

    println("What months within the year 2018 saw the highest number of fire calls?\n")
    fireCallsByMonthIn2018DF
      .show(13, truncate = false)

    // 3 • Which neighborhood in San Francisco generated the most fire calls in 2018?

    val neighborhoodInSFDF = fireTsDF
      .where(col("City") === "San Francisco")
      .where(col("IncidentDate") >= beginningOf2018TS)
      .where(col("IncidentDate") <= endOf2018TS)
      // Group by Neighborhood
      .groupBy(col("Neighborhood"))
      // count all
      .agg(count("*").alias("CallCount"))
      // order by CallCount
      .orderBy(desc("CallCount"))
      // get the biggest - we can also use take(1)
      // limit will return a DF
      .limit(1)

    println("Which neighborhood in San Francisco generated the most fire calls in 2018?\n")
    neighborhoodInSFDF.show()
    // Tenderloin - 1393

    // 4 • Which neighborhoods had the worst response times to fire calls in 2018?
    val worstResponseTimesByNeighborhoodIn2018DF = fireTsDF
      .where(col("IncidentDate") >= beginningOf2018TS)
      .where(col("IncidentDate") <= endOf2018TS)
      .groupBy(col("Neighborhood"))
      .agg(avg("Delay").alias("AverageResponseTime"))
      .orderBy(desc("AverageResponseTime"))

    println("Which neighborhoods had the worst response times to fire calls in 2018?\n")
    worstResponseTimesByNeighborhoodIn2018DF
      .show()

    // 5 • Which week in the year in 2018 had the most fire calls?
    val worstWeekDF = fireTsDF
      .where(col("IncidentDate") >= beginningOf2018TS)
      .where(col("IncidentDate") <= endOf2018TS)
      .groupBy(weekofyear(col("IncidentDate")).alias("Week"))
      // count all of them which is grouped by Weeks
      .agg(count("*").alias("CallCount"))
      .orderBy(desc("CallCount"))

    println("Which week in the year in 2018 had the most fire calls?\n")
    worstWeekDF.show()

    // 6 • Is there a correlation between neighborhood, zip code, and number of fire calls?

    // Aggregate the number of fire calls by neighborhood and zip code
    val fireCallsAgg = fireTsDF
      .groupBy("Neighborhood", "Zipcode")
      .agg(count("*").alias("NumFireCalls"))
      .orderBy(desc("NumFireCalls"))

    // Correlation analysis: One way to statistically analyze categorical
    // data is to use Chi-square test of independence
    // Since Spark MLlib's ChiSquareTest is typically used for
    // labeled data and requires numerical features,
    // we would convert the categorical data to numerical indices (not shown here).
    // For simplicity, let's just display the aggregated data for further external analysis.

    // Show the aggregation
    println("Is there a correlation between neighborhood, zip code, and number of fire calls?")
    println("Watch the relation and comment on it\n")
    fireCallsAgg.show()

    // TODO : Currently correlation calculation for columns with dataType string not supported.
    // val correlationNeighborhoodZipcode = fireTsDF
    //   .stat.corr("Neighborhood", "Zipcode")
    // println(s"Correlation Between Neighborhood and Zipcode $correlationNeighborhoodZipcode")

    // 7 • How can we use Parquet files or SQL tables to store this data and read it back?

    // one by one
    val parquetPath = "/tmp/parquet3"
    fireCallsByMonthIn2018DF
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(parquetPath)

    val SeqOfDataframes = Seq(fireCallsByMonthIn2018DF,
      neighborhoodInSFDF,
      worstResponseTimesByNeighborhoodIn2018DF,
      worstWeekDF,
      fireCallsAgg)

    // parallel writing version - multiple Dataframes written as parquets
    // https://stackoverflow.com/a/73413809
    SeqOfDataframes
      .par
      .zipWithIndex
      .foreach(x => x._1
        .write
        .mode("overwrite")
        .parquet(s"/tmp/dataframe_${x._2}"))

    println(s"You have written all the Dataframes you calculated into /tmp")
    println("Here is one of them, read back")
    // read one bacK
    val readDF = spark
      .read.format("parquet")
      .load("/tmp/dataframe_2")

    readDF.show()

    // Save one as table
    // Careful! It fails if exists

    /*
    val worstWeekParquetTable = "worstWeek" // name of the table
    worstWeek
      .write
      .format("parquet")
      .saveAsTable(worstWeekParquetTable)

    println("Now from table")

    // read from the table
    val readFromTableWorstWeek = spark
      .read
      .table(worstWeekParquetTable)

    readFromTableWorstWeek.show()
    */

  }
}
