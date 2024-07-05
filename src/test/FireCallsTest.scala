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

    // Because we used Option[Datatype]
    // we have to use Some(Datatype) for our mock data

    // How are they related ?

    // In Scala, Option is a type that represents a value that
    // may be present or absent. It is a container that can hold either
    // a value (Some) or no value (None).
    //
    // Using Option helps to avoid null values and the issues associated
    // with them by explicitly handling the presence or absence of a value.
    //
    //  Option Type
    //  Option is a sealed trait with two subclasses: Some and None.
    //
    //  Some represents a value that is present.
    //   None represents the absence of a value.
    //
    //  Why Use Option?
    //  Using Option makes your code safer and more expressive by avoiding
    //  null and clearly indicating when a value might be missing.
    //  This helps prevent NullPointerException and makes it explicit
    //  that a value may or may not be present.
    //
    //  Some
    //  Some is a case class that holds a value. It is used to
    //  wrap a value in an Option.

    //  Why Did We Use Some in the Test Data?
    //  In the case class FireCallInstance, fields are defined as Option[Type].
    //  This indicates that each field can either have a
    //  value (Some(value)) or no value (None). When creating instances
    //  of FireCallInstance for testing, we need to wrap each value
    //  in Some to indicate that the value is present.
    val testData = Seq(
        FireCallInstance(Some(1), Some("M1"), Some(1001), Some("Medical Incident"), Some("01/01/2021"), Some("01/01/2021"), Some("Other"), Some("01/01/2021 01:01:01 AM"), Some("123 Main St"), Some("San Francisco"), Some(94118), Some("B01"), Some("01"), Some("123"), Some("Low"), Some("High"), Some(1), Some(true), Some("Medical"), Some(1), Some("Engine"), Some(1), Some("FPD01"), Some("SD01"), Some("N01"), Some("POINT (37.7749 -122.4194)"), Some("1"), Some(1.2f)),
        FireCallInstance(Some(2), Some("M2"), Some(1002), Some("Fire Incident"), Some("02/01/2021"), Some("02/01/2021"), Some("Other"), Some("02/01/2021 02:02:02 AM"), Some("456 Main St"), Some("San Francisco"), Some(94118), Some("B02"), Some("02"), Some("456"), Some("Medium"), Some("Medium"), Some(2), Some(false), Some("Fire"), Some(2), Some("Truck"), Some(2), Some("FPD02"), Some("SD02"), Some("N02"), Some("POINT (37.7749 -122.4194)"), Some("2"), Some(2.3f)),
        FireCallInstance(Some(3), Some("M3"), Some(1003), Some("False Alarm"), Some("03/01/2021"), Some("03/01/2021"), Some("Other"), Some("03/01/2021 03:03:03 AM"), Some("789 Main St"), Some("San Francisco"), Some(94118), Some("B03"), Some("03"), Some("789"), Some("High"), Some("Low"), Some(3), Some(true), Some("Alarm"), Some(3), Some("Ambulance"), Some(3), Some("FPD03"), Some("SD03"), Some("N03"), Some("POINT (37.7749 -122.4194)"), Some("3"), Some(3.4f))
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