package learningSpark

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.text.SimpleDateFormat

/**
 * Let's now answer some questions that the book gave us as exercise.
 * Now with Dataset API!
 *
 * Exercise keep us alive :)
 */
object FireCallsTrainingQuestionsDataset {

  // We will have a lot of case classes, in which we
  // will use while mapping
  case class CallTypeDistinct(CallType: Option[String])

  case class FireCallsByMonth(Month: Int, CallCount: Long)

  case class FireCallsByNeighborhood(Neighborhood: Option[String],
                                     CallCount: Long)

  case class WorstResponseTimesByNeighborhood(Neighborhood: Option[String],
                                              AverageResponseTime: Double)

  case class SpecialFireCallInstance(Neighborhood: Option[String],
                                     Delay: Double)

  case class DelaySumCount(sum: Double, count: Long)

  case class WorstWeek(Week: Int, CallCount: Long)

  case class WeekCount(Week: Int, Count: Long)

  case class NeighborhoodZipcodeCount(Neighborhood: String,
                                      Zipcode: Int, Count: Long)

  /** Custom Aggregator to count fire calls by month
   * Input: FireCallsByMonth
   * Buffer: Long
   * Output: Long
   */
  object FireCallsByMonthAggregator extends Aggregator[FireCallsByMonth, Long, Long] {
    def zero: Long = 0L
    def reduce(buffer: Long, call: FireCallsByMonth): Long = buffer + call.CallCount
    def merge(b1: Long, b2: Long): Long = b1 + b2
    def finish(reduction: Long): Long = reduction
    def bufferEncoder: Encoder[Long] = Encoders.scalaLong
    def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  /** Custom Aggregator to calculate count of calls by Neighborhood
   * Input: FireCallsByNeighborhood
   * Buffer: Long
   * Output: Long
   */
  object FireCallsByNeighborhoodAggregator extends Aggregator[FireCallsByNeighborhood, Long, Long]{
    def zero: Long = 0L
    def reduce(buffer: Long, call: FireCallsByNeighborhood): Long = buffer + call.CallCount
    def merge(b1: Long, b2: Long): Long = b1 + b2
    def finish(reduction: Long): Long = reduction
    def bufferEncoder: Encoder[Long] = Encoders.scalaLong
    def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  /** Custom Aggregator to calculate Average Delay
   * Input: SpecialFireCallInstance
   * Buffer: DelaySumCount
   * Output: Double
   */
  object AverageDelayAggregator extends Aggregator[SpecialFireCallInstance, DelaySumCount, Double] {
    def zero: DelaySumCount = DelaySumCount(0.0, 0L)
    def reduce(buffer: DelaySumCount, call: SpecialFireCallInstance): DelaySumCount =
    {
      DelaySumCount(buffer.sum + call.Delay, buffer.count + 1)
    }
    //buffer.sum + call.Delay
    def merge(b1: DelaySumCount, b2: DelaySumCount): DelaySumCount = {
      DelaySumCount(b1.sum + b2.sum, b1.count + b2.count)
    }
    def finish(reduction: DelaySumCount): Double = {
      if (reduction.count == 0) 0.0 else reduction.sum / reduction.count
    }
    def bufferEncoder: Encoder[DelaySumCount] = Encoders.product
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  /**
   * Custom Aggregator to count calls by week
   * Input: WeekCount
   * Buffer: Long
   * Output: Long
   */
  object CallsByWeekAggregator extends Aggregator[WeekCount, Long, Long] {
    def zero: Long = 0L
    def reduce(buffer: Long, weekCount: WeekCount): Long = buffer + weekCount.Count
    def merge(b1: Long, b2: Long): Long = b1 + b2
    def finish(reduction: Long): Long = reduction
    def bufferEncoder: Encoder[Long] = Encoders.scalaLong
    def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  /** Count Calls by Zipcode and Neighborhood
   * Input: NeighborhoodZipcodeCount
   * Buffer: Long
   * Output: Long
   */
  object FireCallsAggregator extends Aggregator[NeighborhoodZipcodeCount, Long, Long] {
    def zero: Long = 0L
    def reduce(buffer: Long, call: NeighborhoodZipcodeCount): Long = buffer + call.Count
    def merge(b1: Long, b2: Long): Long = b1 + b2
    def finish(reduction: Long): Long = reduction
    def bufferEncoder: Encoder[Long] = Encoders.scalaLong
    def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Workout")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val fireCallsPath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-fire-calls.csv"
    }

    val fireDS = spark
      .read
      .format("csv")
      .option("header", value = true)
      .schema(implicitly[Encoder[FireCallInstance]].schema)
      .load(fireCallsPath)
      .as[FireCallInstance]

    // Date format for conversion
    val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
    val timestampFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a")

    // let's make a Firecall dataset with timestamps!
    val fireTsDS = fireDS
      .map(call => mapFireCallsToFireCallsTimestamp(call, dateFormat, timestampFormat))

    // we will repeatedly use fireTsDS
    fireTsDS.cache()

    println("\nDS with timestamps\n")
    fireTsDS.show()

    val endOf2018TS = new Timestamp(dateFormat.parse("12/31/2018").getTime)
    val beginningOf2018TS = new Timestamp(dateFormat.parse("01/01/2018").getTime)

    // 1 • What were all the different types of fire calls in 2018?
    val distinctTypesOfFireCallsDS = fireTsDS
      .filter(call => call.IncidentDate.exists(date =>
        date.after(beginningOf2018TS) && date.before(endOf2018TS)))
      .map(value => CallTypeDistinct(value.CallType))
      .distinct()

    println("distinctTypesOfFireCalls in 2018\n")
    distinctTypesOfFireCallsDS.show(100)

    // 2 • What months within the year 2018 saw the highest number of fire calls?
    val fireCallsByMonthIn2018DS = fireTsDS
      .filter(call => call.IncidentDate.exists(date =>
        date.after(beginningOf2018TS) && date.before(endOf2018TS)))
      // get months with a map method from all calls, count them as 1
      .map(call => FireCallsByMonth(new SimpleDateFormat("MM").format(call.IncidentDate.get).toInt,
        1))
      // group by Month
      .groupByKey(call => call.Month)
      // aggregate with FireCallsByMonthAggregator
      .agg(FireCallsByMonthAggregator.toColumn)
      // map it back to FireCallsByMonth
      .map { case (month, count) => FireCallsByMonth(month, count) }
      .orderBy(desc("CallCount"))

    println("What months within the year 2018 saw the highest number of fire calls?\n")
    fireCallsByMonthIn2018DS.show(13, truncate = false)

    // 3 • Which neighborhood in San Francisco generated the most fire calls in 2018?
    val neighborhoodInSFDS = fireTsDS
      .filter(call => call.City.contains("San Francisco"))
      .filter(call => call.IncidentDate.exists(date =>
        date.after(beginningOf2018TS) && date.before(endOf2018TS)))
      .map(call => FireCallsByNeighborhood(call.Neighborhood, 1))
      .groupByKey(call => call.Neighborhood)
      .agg(FireCallsByNeighborhoodAggregator.toColumn)
      .map { case (neighborhood, count) => FireCallsByNeighborhood(neighborhood, count) }
      .orderBy(desc("CallCount"))
      .limit(1)

    println("Which neighborhood in San Francisco generated the most fire calls in 2018?\n")
    neighborhoodInSFDS.show()

    // 4 • Which neighborhoods had the worst response times to fire calls in 2018?
    val worstResponseTimesByNeighborhoodIn2018DS = fireTsDS
      .filter(call => call.IncidentDate.exists(date =>
        !date.before(beginningOf2018TS) && !date.after(endOf2018TS)))
      .map(call => SpecialFireCallInstance(call.Neighborhood,
        call.Delay.getOrElse(0.0)))
      .groupByKey(call => call.Neighborhood)
      .agg(AverageDelayAggregator.toColumn)
      .map { case (neigh, avgDelay) => WorstResponseTimesByNeighborhood(neigh, avgDelay) }
      .orderBy(desc("AverageResponseTime"))

    println("Which neighborhoods had the worst response times to fire calls in 2018?\n")
    worstResponseTimesByNeighborhoodIn2018DS.show()

    // 5 • Which week in the year in 2018 had the most fire calls?

    val worstWeekDS = fireTsDS
      .filter(call => call.IncidentDate.exists(date =>
        !date.before(beginningOf2018TS) && !date.after(endOf2018TS)))
      // map inside map!
      // from calls, get the Incident Date
      // and map it to the week of the year!
      .map(call => WeekCount(call.IncidentDate.map(date =>
        new SimpleDateFormat("w").format(date).toInt).getOrElse(0),
        1L))
      .groupByKey(call => call.Week)
      .agg(CallsByWeekAggregator.toColumn)
      .map { case (week, count) => WorstWeek(week, count) }
      .orderBy(desc("CallCount"))

    // This is also a valid approach for Question 5

    // val worstWeekDS = fireTsDS
    //   .filter(call => call.IncidentDate.exists(date =>
    //     !date.before(beginningOf2018TS) && !date.after(endOf2018TS)))
    //   .flatMap(call => call.IncidentDate.map(date => WeekCount(new SimpleDateFormat("w").format(date).toInt, 1)))
    //   .groupByKey(call => call.Week)
    //   .agg(CallsByWeekAggregator.toColumn)
    //   .map { case (week, count) => WorstWeek(week, count) }
    //   .orderBy(desc("CallCount"))

    println("Which week in the year in 2018 had the most fire calls?\n")
    worstWeekDS.show()

    // 6 • Is there a correlation between neighborhood, zip code, and number of fire calls?
    val fireCallsAggDS = fireTsDS
      .filter(call => call.Neighborhood.isDefined && call.Zipcode.isDefined)
      .map(call => NeighborhoodZipcodeCount(call.Neighborhood.get, call.Zipcode.get, 1L))
      .groupByKey(call => (call.Neighborhood, call.Zipcode))
      .agg(FireCallsAggregator.toColumn)
      .map { case ((neighborhood, zipcode), count) => FireCallsAgg(neighborhood, zipcode, count) }
      .orderBy(desc("NumFireCalls"))


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

    val SeqOfDatasets = Seq(
      fireCallsByMonthIn2018DS,
      neighborhoodInSFDS,
      worstResponseTimesByNeighborhoodIn2018DS,
      worstWeekDS,
      fireCallsAggDS
    )

    // parallel writing version - multiple Dataframes written as parquets
    // https://stackoverflow.com/a/73413809
    SeqOfDatasets
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
    val readDS = spark
      .read.format("parquet")
      .load("/tmp/dataset_2")

    readDS.show()
  }

  /**
   * A simple mapping method to generate
   * FireCallInstanceWithTimestamps instances
   * @param call: A FireCallInstance
   * @param dateFormat: SimpleDateFormat
   * @param timestampFormat: SimpleDateFormat
   * @return
   */
  def mapFireCallsToFireCallsTimestamp(call: FireCallInstance,
                                       dateFormat: SimpleDateFormat,
                                       timestampFormat: SimpleDateFormat
                                      ): FireCallInstanceWithTimestampsWithoutRename = {
    FireCallInstanceWithTimestampsWithoutRename(
      call.CallNumber,
      call.UnitID,
      call.IncidentNumber,
      call.CallType,
      call.CallDate.map(d => new Timestamp(dateFormat.parse(d).getTime)),
      call.WatchDate.map(d => new Timestamp(dateFormat.parse(d).getTime)),
      call.CallFinalDisposition,
      call.AvailableDtTm.map(d => new Timestamp(timestampFormat.parse(d).getTime)),
      call.Address,
      call.City,
      call.Zipcode,
      call.Battalion,
      call.StationArea,
      call.Box,
      call.OriginalPriority,
      call.Priority,
      call.FinalPriority,
      call.ALSUnit,
      call.CallTypeGroup,
      call.NumAlarms,
      call.UnitType,
      call.UnitSequenceInCallDispatch,
      call.FirePreventionDistrict,
      call.SupervisorDistrict,
      call.Neighborhood,
      call.Location,
      call.RowID,
      call.Delay,
    )
  }
}
