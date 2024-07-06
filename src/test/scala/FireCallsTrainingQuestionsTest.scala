package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class FireCallsTrainingQuestionsTest extends AnyFunSuite{

  val spark: SparkSession = SparkSession
    .builder
    .appName("FireCallsTrainingQuestionsDatasetTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // bunch of data cherry picked from 2018
  val testData: Dataset[FireCallInstanceWithTimestampsWithoutRename] = Seq(
    FireCallInstanceWithTimestampsWithoutRename(Some(182953294), Some("74"), Some(18123778), Some("Medical Incident"), Some(Timestamp.valueOf("2018-10-22 18:59:03")), Some(Timestamp.valueOf("2018-10-22 00:00:00")), Some(Timestamp.valueOf("2018-10-22 18:59:03")), Some("WHITNEY YOUNG CR/HUDSON AV"), Some("San Francisco"), Some(94124), Some("B10"), Some("17"), Some("6621"), Some("2"), Some("2"), Some(2), Some(true), Some("Potentially Life-Threatening"), Some(1), Some("MEDIC"), Some(1), Some("10"), Some("10"), Some("Bayview Hunters Point"), Some("(37.73363179498058, -122.3822793513502)"), Some("182953294-74"), Some(3.2333333)),
    FireCallInstanceWithTimestampsWithoutRename(Some(182953352), Some("54"), Some(18123782), Some("Medical Incident"), Some(Timestamp.valueOf("2018-10-22 20:28:59")), Some(Timestamp.valueOf("2018-10-22 00:00:00")), Some(Timestamp.valueOf("2018-10-22 20:28:59")), Some("MISSOURI ST/18TH ST"), Some("San Francisco"), Some(94107), Some("B03"), Some("37"), Some("2462"), Some("3"), Some("2"), Some(2), Some(true), Some("Non Life-threatening"), Some(1), Some("MEDIC"), Some(2), Some("3"), Some("10"), Some("Potrero Hill"), Some("(37.762579649962866, -122.39652682672629)"), Some("182953352-54"), Some(1.0)),
    FireCallInstanceWithTimestampsWithoutRename(Some(182953365), Some("B03"), Some(18123783), Some("Alarms"), Some(Timestamp.valueOf("2018-10-22 19:27:45")), Some(Timestamp.valueOf("2018-10-22 00:00:00")), Some(Timestamp.valueOf("2018-10-22 19:27:45")), Some("700 Block of FOLSOM ST"), Some("San Francisco"), Some(94107), Some("B03"), Some("01"), Some("2215"), Some("3"), Some("3"), Some(3), Some(false), Some("Alarm"), Some(1), Some("CHIEF"), Some(2), Some("3"), Some("6"), Some("South of Market"), Some("(37.78229545702615, -122.40071823095856)"), Some("182953365-B03"), Some(2.95)),
    FireCallInstanceWithTimestampsWithoutRename(Some(182974142), Some("E26"), Some(18124671), Some("Structure Fire"), Some(Timestamp.valueOf("2018-10-24 23:28:25")), Some(Timestamp.valueOf("2018-10-24 00:00:00")), Some(Timestamp.valueOf("2018-10-24 23:28:25")), Some("0 Block of STONEYBROOK AV"), Some("San Francisco"), Some(94112), Some("B09"), Some("32"), Some("5681"), Some("3"), Some("3"), Some(3), Some(true), Some("Fire"), Some(1), Some("ENGINE"), Some(7), Some("9"), Some("11"), Some("Excelsior"), Some("(37.73007601747232, -122.42176418389366)"), Some("182974142-E26"), Some(1.2)),
    FireCallInstanceWithTimestampsWithoutRename(Some(182980176), Some("62"), Some(18124731), Some("Medical Incident"), Some(Timestamp.valueOf("2018-10-25 02:59:06")), Some(Timestamp.valueOf("2018-10-24 00:00:00")), Some(Timestamp.valueOf("2018-10-25 02:59:06")), Some("1300 Block of 9TH AVE"), Some("San Francisco"), Some(94122), Some("B08"), Some("22"), Some("7334"), Some("3"), Some("3"), Some(3), Some(true), Some("Potentially Life-Threatening"), Some(1), Some("MEDIC"), Some(1), Some("8"), Some("5"), Some("Inner Sunset"), Some("(37.76310297616702, -122.4663146686432)"), Some("182980176-62"), Some(1.45)),
    FireCallInstanceWithTimestampsWithoutRename(Some(182991371), Some("73"), Some(18125269), Some("Traffic Collision"), Some(Timestamp.valueOf("2018-10-26 11:35:35")), Some(Timestamp.valueOf("2018-10-26 00:00:00")), Some(Timestamp.valueOf("2018-10-26 11:35:35")), Some("22ND ST/SOUTH VAN NESS AV"), Some("San Francisco"), Some(94110), Some("B06"), Some("07"), Some("5473"), Some("A"), Some("3"), Some(3), Some(true), Some("Potentially Life-Threatening"), Some(1), Some("MEDIC"), Some(2), Some("6"), Some("9"), Some("Mission"), Some("(37.75556827812936, -122.41656664305323)"), Some("182991371-73"), Some(4.1666665)),
    FireCallInstanceWithTimestampsWithoutRename(Some(182991660), Some("QRV2"), Some(18125301), Some("Medical Incident"), Some(Timestamp.valueOf("2018-10-26 12:12:57")), Some(Timestamp.valueOf("2018-10-26 00:00:00")), Some(Timestamp.valueOf("2018-10-26 12:12:57")), Some("1100 Block of CALIFORNIA ST"), Some("San Francisco"), Some(94108), Some("B01"), Some("41"), Some("1445"), Some("2"), Some("2"), Some(2), Some(true), Some("Potentially Life-Threatening"), Some(1), Some("SUPPORT"), Some(1), Some("1"), Some("3"), Some("Nob Hill"), Some("(37.79157600618609, -122.4132951088654)"), Some("182991660-QRV2"), Some(6.866667)),
    FireCallInstanceWithTimestampsWithoutRename(Some(182991686), Some("E06"), Some(18125303), Some("Structure Fire"), Some(Timestamp.valueOf("2018-10-26 12:11:29")), Some(Timestamp.valueOf("2018-10-26 00:00:00")), Some(Timestamp.valueOf("2018-10-26 12:11:29")), Some("2900 Block of 16TH ST"), Some("San Francisco"), Some(94110), Some("B02"), Some("07"), Some("5236"), Some("3"), Some("3"), Some(3), Some(true), Some("Alarm"), Some(1), Some("ENGINE"), Some(2), Some("2"), Some("9"), Some("Mission"), Some("(37.765095380734294, -122.41802762014147)"), Some("182991686-E06"), Some(4.483333)),
    FireCallInstanceWithTimestampsWithoutRename(Some(183023492), Some("E42"), Some(18126782), Some("Structure Fire"), Some(Timestamp.valueOf("2018-10-29 19:01:55")), Some(Timestamp.valueOf("2018-10-29 00:00:00")), Some(Timestamp.valueOf("2018-10-29 19:01:55")), Some("1100 Block of FITZGERALD AVE"), Some("San Francisco"), Some(94124), Some("B10"), Some("17"), Some("6615"), Some("3"), Some("3"), Some(3), Some(true), Some("Alarm"), Some(1), Some("ENGINE"), Some(4), Some("10"), Some("10"), Some("Bayview Hunters Point"), Some("(37.72040701923885, -122.39042296087085)"), Some("183023492-E42"), Some(2.2333333)),
    FireCallInstanceWithTimestampsWithoutRename(Some(183023528), Some("E43"), Some(18126786), Some("Odor (Strange / Unknown)"), Some(Timestamp.valueOf("2018-10-29 19:21:39")), Some(Timestamp.valueOf("2018-10-29 00:00:00")), Some(Timestamp.valueOf("2018-10-29 19:21:39")), Some("400 Block of EDINBURGH ST"), Some("San Francisco"), Some(94112), Some("B09"), Some("43"), Some("6141"), Some("3"), Some("3"), Some(3), Some(true), Some("Alarm"), Some(1), Some("ENGINE"), Some(1), Some("9"), Some("11"), Some("Excelsior"), Some("(37.7220390695528, -122.43147855442646)"), Some("183023528-E43"), Some(4.6)),
    FireCallInstanceWithTimestampsWithoutRename(Some(183023805), Some("58"), Some(18126810), Some("Medical Incident"), Some(Timestamp.valueOf("2018-10-29 21:42:57")), Some(Timestamp.valueOf("2018-10-29 00:00:00")), Some(Timestamp.valueOf("2018-10-29 21:42:57")), Some("MISSION ST/16TH ST"), Some("San Francisco"), Some(94103), Some("B02"), Some("07"), Some("5236"), Some("2"), Some("2"), Some(2), Some(true), Some("Non Life-threatening"), Some(1), Some("MEDIC"), Some(2), Some("2"), Some("9"), Some("Mission"), Some("(37.765051338194475, -122.41966897386114)"), Some("183023805-58"), Some(3.3166666)),
    FireCallInstanceWithTimestampsWithoutRename(Some(183023815), Some("T05"), Some(18126812), Some("Alarms"), Some(Timestamp.valueOf("2018-10-29 20:40:46")), Some(Timestamp.valueOf("2018-10-29 00:00:00")), Some(Timestamp.valueOf("2018-10-29 20:40:46")), Some("2000 Block of UNION ST"), Some("San Francisco"), Some(94123), Some("B04"), Some("16"), Some("3443"), Some("3"), Some("3"), Some(3), Some(false), Some("Alarm"), Some(1), Some("TRUCK"), Some(3), Some("4"), Some("2"), Some("Marina"), Some("(37.79745672495492, -122.43303646999108)"), Some("183023815-T05"), Some(2.7833333))
  ).toDS()

  test("Distinct types of fire calls in 2018") {
    val distinctTypesOfFireCallsDS = testData
      .filter(col("IncidentDate") >= Timestamp.valueOf("2018-01-01 00:00:00"))
      .filter(col("IncidentDate") <= Timestamp.valueOf("2018-12-31 23:59:59"))
      .select(col("CallType"))
      .distinct()
      .as[String]

    // in our data, we have these
    val expected = Set("Medical Incident", "Alarms", "Structure Fire", "Traffic Collision", "Odor (Strange / Unknown)")
    val result = distinctTypesOfFireCallsDS.collect().toSet

    // they should be equal
    // In Scala == is equals, === is equalsTo
    // Here is more: https://stackoverflow.com/a/43438347
    assert(result == expected)
  }

  test("Months with the highest number of fire calls in 2018") {
    val fireCallsByMonthIn2018DS = testData
      // start and end of the year
      .filter(col("IncidentDate") >= Timestamp.valueOf("2018-01-01 00:00:00"))
      .filter(col("IncidentDate") <= Timestamp.valueOf("2018-12-31 23:59:59"))
      .groupBy(month(col("IncidentDate")).alias("Month"))
      .agg(count("*").alias("CallCount"))
      .orderBy(desc("CallCount"))
      .as[FireCallsByMonth]

    // our test data has 12 instances of month 10
    val expected = Seq(FireCallsByMonth(10, 12))
    val result = fireCallsByMonthIn2018DS.collect()
    // we expect them to be the same
    assert(result.sameElements(expected))
  }

  test("Neighborhood with the most fire calls in 2018") {
    val neighborhoodInSFDS = testData
      .filter(col("City") === "San Francisco")
      .filter(col("IncidentDate") >= Timestamp.valueOf("2018-01-01 00:00:00"))
      .filter(col("IncidentDate") <= Timestamp.valueOf("2018-12-31 23:59:59"))
      .groupBy(col("Neighborhood"))
      .agg(count("*").alias("CallCount"))
      .orderBy(desc("CallCount"))
      .limit(1)
      .as[NeighborhoodFireCalls]

    // in our data, Mission has 3 instances
    val expected = NeighborhoodFireCalls("Mission", 3)
    val result = neighborhoodInSFDS.collect().head
    assert(result == expected)
  }

  test("Neighborhoods with the worst response times in 2018") {
    val worstResponseTimesByNeighborhoodIn2018DS = testData
      .filter(col("IncidentDate") >= Timestamp.valueOf("2018-01-01 00:00:00"))
      .filter(col("IncidentDate") <= Timestamp.valueOf("2018-12-31 23:59:59"))
      .groupBy(col("Neighborhood"))
      .agg(avg("Delay").alias("AverageResponseTime"))
      .orderBy(desc("AverageResponseTime"))
      .as[WorstResponseTimesByNeighborhood]

    val expected = Array(
      WorstResponseTimesByNeighborhood("Nob Hill", 6.866667),
      WorstResponseTimesByNeighborhood("Mission", 3.9888887),
      WorstResponseTimesByNeighborhood("South of Market", 2.95),
      WorstResponseTimesByNeighborhood("Excelsior", 2.9),
      WorstResponseTimesByNeighborhood("Marina", 2.7833333),
      WorstResponseTimesByNeighborhood("Bayview Hunters Point", 2.7333333),
      WorstResponseTimesByNeighborhood("Inner Sunset", 1.45),
      WorstResponseTimesByNeighborhood("Potrero Hill", 1.0))

    val result = worstResponseTimesByNeighborhoodIn2018DS.collect()

    assert(result === expected)
    // we can use this too
    // assert(result.sameElements(expected))

  }

  test("Week with the most fire calls in 2018") {
    val worstWeekDS = testData
      .filter(col("IncidentDate") >= Timestamp.valueOf("2018-01-01 00:00:00"))
      .filter(col("IncidentDate") <= Timestamp.valueOf("2018-12-31 23:59:59"))
      .groupBy(weekofyear(col("IncidentDate")).alias("Week"))
      .agg(count("*").alias("CallCount"))
      .orderBy(desc("CallCount"))
      .as[WorstWeek]

    // 8 instances from week 43, 4 instances from week 44
    val expected = Array(WorstWeek(43, 8), WorstWeek(44, 4))
    val result = worstWeekDS.collect()
    assert(result === expected)
    // we can use this too
    // assert(result.sameElements(expected))
  }

  test("Correlation between neighborhood, zip code, and number of fire calls") {
    val fireCallsAggDS = testData
      .groupBy("Neighborhood", "Zipcode")
      .agg(count("*").alias("NumFireCalls"))
      .orderBy(desc("NumFireCalls"))
      .as[FireCallsAgg]

    // Result will be ordered descending by NumFireCalls
    val expected = Array(
      FireCallsAgg("Bayview Hunters Point", 94124, 2),
      FireCallsAgg("Excelsior", 94112, 2),
      FireCallsAgg("Mission", 94110, 2),
      FireCallsAgg("Potrero Hill", 94107, 1),
      FireCallsAgg("South of Market", 94107, 1),
      FireCallsAgg("Inner Sunset", 94122, 1),
      FireCallsAgg("Nob Hill", 94108, 1),
      FireCallsAgg("Mission", 94103, 1),
      FireCallsAgg("Marina", 94123, 1)
    )
    val result = fireCallsAggDS.collect()
    assert(result === expected)
  }
}
