package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

class FireCallsTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("FireCallsDatasetTest")
    .getOrCreate()

  import spark.implicits._

  test("Timestamp conversion and filtering") {

    val testData = Seq(
        FireCallInstance(Some(1), Some("M1"), Some(1001), Some("Medical Incident"), Some("01/01/2021"), Some("01/01/2021"), Some("Other"), Some("01/01/2021 01:01:01 AM"), Some("123 Main St"), Some("San Francisco"), Some(94118), Some("B01"), Some("01"), Some("123"), Some("Low"), Some("High"), Some(1), Some(true), Some("Medical"), Some(1), Some("Engine"), Some(1), Some("FPD01"), Some("SD01"), Some("N01"), Some("POINT (37.7749 -122.4194)"), Some("1"), Some(1.2)),
        FireCallInstance(Some(2), Some("M2"), Some(1002), Some("Fire Incident"), Some("02/01/2021"), Some("02/01/2021"), Some("Other"), Some("02/01/2021 02:02:02 AM"), Some("456 Main St"), Some("San Francisco"), Some(94118), Some("B02"), Some("02"), Some("456"), Some("Medium"), Some("Medium"), Some(2), Some(false), Some("Fire"), Some(2), Some("Truck"), Some(2), Some("FPD02"), Some("SD02"), Some("N02"), Some("POINT (37.7749 -122.4194)"), Some("2"), Some(2.3)),
        FireCallInstance(Some(3), Some("M3"), Some(1003), Some("False Alarm"), Some("03/01/2021"), Some("03/01/2021"), Some("Other"), Some("03/01/2021 03:03:03 AM"), Some("789 Main St"), Some("San Francisco"), Some(94118), Some("B03"), Some("03"), Some("789"), Some("High"), Some("Low"), Some(3), Some(true), Some("Alarm"), Some(3), Some("Ambulance"), Some(3), Some("FPD03"), Some("SD03"), Some("N03"), Some("POINT (37.7749 -122.4194)"), Some("3"), Some(3.4))
      ).toDS()

    val updatedFireCallsDS = testData
        .withColumn("CallDateTimestamp", unix_timestamp($"CallDate", "dd/MM/yyyy").cast("timestamp"))
        .withColumn("CallDateTimestampTwo", to_timestamp($"CallDate", "dd/MM/yyyy"))

    val selection = updatedFireCallsDS
        .select("CallDate", "StationArea", "Zipcode", "CallDateTimestamp", "CallDateTimestampTwo")
        .filter($"CallDateTimestamp".isNotNull)
        .filter($"CallDateTimestampTwo".isNotNull)

    val orderedSelection = selection
        .where("Zipcode = 94118")
        .orderBy("CallDateTimestamp")
        .select("CallDateTimestamp", "StationArea", "Zipcode", "CallDateTimestampTwo")

    val result = orderedSelection.collect()

    assert(result.length == 3)
    assert(result(0).getTimestamp(0).toString == "2021-01-01 00:00:00.0")
    assert(result(0).getTimestamp(3).toString == "2021-01-01 00:00:00.0")
    assert(result(1).getTimestamp(0).toString == "2021-01-02 00:00:00.0")
    assert(result(1).getTimestamp(3).toString == "2021-01-02 00:00:00.0")
    assert(result(2).getTimestamp(0).toString == "2021-01-03 00:00:00.0")
    assert(result(2).getTimestamp(3).toString == "2021-01-03 00:00:00.0")
  }
}