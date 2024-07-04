package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

class MnmCandiesTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("MnmCandiesTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("Aggregation by State and Color") {

    // Make a test DataFrame
    val testDF = Seq(
      Mnm("CA", "Red", 10),
      Mnm("CA", "Blue", 20),
      Mnm("TX", "Red", 15),
      Mnm("TX", "Blue", 5),
      Mnm("NY", "Red", 25)
    ).toDF()

    // Perform the aggregation
    val countMnMD = testDF
      .select($"State", $"Color", $"Count")
      .groupBy($"State", $"Color")
      .agg(count($"Count").alias("Total"))
      .orderBy($"Total".desc)

    // Collect the result
    val result = countMnMD.collect()

    // Expected result
    val expectedResult = Seq(
      ("NY", "Red", 1),
      ("CA", "Blue", 1),
      ("CA", "Red", 1),
      ("TX", "Red", 1),
      ("TX", "Blue", 1)
    ).toArray

    // Assert the result matches the expected result
    // Because the Result is an Array[Row], we have to map it into each element
    assert(result.map(r => (r.getString(0), r.getString(1), r.getLong(2))).sorted === expectedResult.sorted)
  }

  test("Aggregation for California") {
    // Create test DataFrame
    val testDf = Seq(
      Mnm("CA", "Red", 10),
      Mnm("CA", "Blue", 20),
      Mnm("TX", "Red", 15),
      Mnm("TX", "Blue", 5),
      Mnm("NY", "Red", 25)
    ).toDF()

    // Perform the aggregation for California
    val californiaMnMDF = testDf
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    // Collect the result
    val result = californiaMnMDF.collect()

    // Expected result
    val expectedResult = Seq(
      ("CA", "Blue", 1),
      ("CA", "Red", 1)
    ).toArray

    // Assert the result matches the expected result
    assert(result.map(r => (r.getString(0), r.getString(1), r.getLong(2))).sorted === expectedResult.sorted)
  }

  test("Discover all alternatives!"){

    // we will use a lot of different data structures made from Sequences,
    // in our Source code as well as our Tests.
    // so lets discover them!

    // make a sequence
    val seq = Seq(("CA", "Red", 1),
                  ("CA", "Blue", 1),
                  ("TX", "Red", 1),
                  ("TX", "Blue", 1),
                  ("NY", "Red", 1))

    // We can Convert to List
    val listResult: List[(String, String, Int)] = seq.toList

    // We can Convert to Array
    val arrayResult: Array[(String, String, Int)] = seq.toArray

    // We can Convert to Vector
    val vectorResult: Vector[(String, String, Int)] = seq.toVector

    // We can Convert to Set
    val setResult: Set[(String, String, Int)] = seq.toSet

    // We can Convert to Map
    val mapResult: Map[(String, String), Int] = seq.map { case (state, color, count) => ((state, color), count) }.toMap

    // We can Convert to Mutable ListBuffer
    import scala.collection.mutable.ListBuffer
    val listBufferResult: ListBuffer[(String, String, Int)] = ListBuffer(seq: _*)

    // We can Convert to Mutable ArrayBuffer
    import scala.collection.mutable.ArrayBuffer
    val arrayBufferResult: ArrayBuffer[(String, String, Int)] = ArrayBuffer(seq: _*)
  }

}