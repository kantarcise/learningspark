package learningSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

// Same questions, with Dataset API
object FireCallsTrainingQuestionsDataset {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Workout")
      .master("local")
      .getOrCreate()

    // verbose = False
    spark.sparkContext.setLogLevel("ERROR")

    // dollar sign usage
    import spark.implicits._

    // Define the data path as a val
    val fireCallsPath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-fire-calls.csv"
    }

    // the same schema
    val fireSchema = StructType(
      Array(
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
        StructField("Delay", DoubleType, nullable = true)
      )
    )

    val fireDF = spark
      .read
      .format("csv")
      .option("header", value = true)
      .schema(fireSchema)
      .load(fireCallsPath)

    // for to_timestamp, we can define patterns as
    val simpleTimePattern = "MM/dd/yyyy"
    val complexTimePattern = "MM/dd/yyyy hh:mm:ss a"

    // before start, get to the timestamps
    val fireTsDS = fireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), simpleTimePattern))
      .drop($"CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), simpleTimePattern))
      .drop($"WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), complexTimePattern))
      .drop($"AvailableDtTm")
      .as[FireCallInstanceWithTimestampsWithoutRename]

    println("DS with timestamps\n")
    fireTsDS.show()

    val endOf2018TS = to_timestamp(lit("12/31/2018"), simpleTimePattern)
    val beginningOf2018TS = to_timestamp(lit("01/01/2018"), simpleTimePattern)

    // 1 • What were all the different types of fire calls in 2018?
    val distinctTypesOfFireCallsDS = fireTsDS
      .filter(col("IncidentDate") >= beginningOf2018TS)
      .filter(col("IncidentDate") <= endOf2018TS)
      .select(col("CallType"))
      .distinct()
      .as[String]

    println("distinctTypesOfFireCalls in 2018\n")
    distinctTypesOfFireCallsDS.show(100)

    // 2 • What months within the year 2018 saw the highest number of fire calls?
    val fireCallsByMonthIn2018DS = fireTsDS
      .filter(col("IncidentDate") >= beginningOf2018TS)
      .filter(col("IncidentDate") <= endOf2018TS)
      .groupBy(month(col("IncidentDate")).alias("Month"))
      .agg(count("*").alias("CallCount"))
      .orderBy(desc("CallCount"))
      .as[FireCallsByMonth]

    println("What months within the year 2018 saw the highest number of fire calls?\n")
    fireCallsByMonthIn2018DS.show(13, truncate = false)

    // 3 • Which neighborhood in San Francisco generated the most fire calls in 2018?
    val neighborhoodInSFDS = fireTsDS
      .filter(col("City") === "San Francisco")
      .filter(col("IncidentDate") >= beginningOf2018TS)
      .filter(col("IncidentDate") <= endOf2018TS)
      .groupBy(col("Neighborhood"))
      .agg(count("*").alias("CallCount"))
      .orderBy(desc("CallCount"))
      .limit(1)
      .as[NeighborhoodFireCalls]

    println("Which neighborhood in San Francisco generated the most fire calls in 2018?\n")
    neighborhoodInSFDS.show()

    // 4 • Which neighborhoods had the worst response times to fire calls in 2018?
    val worstResponseTimesByNeighborhoodIn2018DS = fireTsDS
      .filter(col("IncidentDate") >= beginningOf2018TS)
      .filter(col("IncidentDate") <= endOf2018TS)
      .groupBy(col("Neighborhood"))
      .agg(avg("Delay").alias("AverageResponseTime"))
      .orderBy(desc("AverageResponseTime"))
      .as[WorstResponseTimesByNeighborhood]

    println("Which neighborhoods had the worst response times to fire calls in 2018?\n")
    worstResponseTimesByNeighborhoodIn2018DS.show()

    // 5 • Which week in the year in 2018 had the most fire calls?
    val worstWeekDS = fireTsDS
      .filter(col("IncidentDate") >= beginningOf2018TS)
      .filter(col("IncidentDate") <= endOf2018TS)
      .groupBy(weekofyear(col("IncidentDate")).alias("Week"))
      .agg(count("*").alias("CallCount"))
      .orderBy(desc("CallCount"))
      .as[WorstWeek]

    println("Which week in the year in 2018 had the most fire calls?\n")
    worstWeekDS.show()

    // 6 • Is there a correlation between neighborhood, zip code, and number of fire calls?
    val fireCallsAggDS = fireTsDS
      .groupBy("Neighborhood", "Zipcode")
      .agg(count("*").alias("NumFireCalls"))
      .orderBy(desc("NumFireCalls"))
      .as[FireCallsAgg]

    println("Is there a correlation between neighborhood, zip code, and number of fire calls?")
    println("Watch the relation and comment on it\n")
    fireCallsAggDS.show()

    // 7 • How can we use Parquet files or SQL tables to store this data and read it back?
    val parquetPath = "/tmp/parquet4"
    fireCallsByMonthIn2018DS
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(parquetPath)

    val SeqOfDataframes = Seq(
      fireCallsByMonthIn2018DS,
      neighborhoodInSFDS,
      worstResponseTimesByNeighborhoodIn2018DS,
      worstWeekDS,
      fireCallsAggDS
    )

    // parallel writing version - multiple Dataframes written as parquets
    // https://stackoverflow.com/a/73413809
    SeqOfDataframes
      .par
      .zipWithIndex
      .foreach { case (ds, idx) => ds
        .write
        .mode("overwrite")
        .parquet(s"/tmp/dataset_${idx}")
      }

    println(s"You have written all the Datasets you calculated into /tmp")
    println("Here is one of them, read back")
    // read one back
    val readDF = spark
      .read.format("parquet")
      .load("/tmp/dataset_2")

    readDF.show()
  }
}
