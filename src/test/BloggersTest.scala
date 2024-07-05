package learningSpark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class BloggersTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("BloggersTest")
    .getOrCreate()

  import spark.implicits._

  // lets make a test data with BlogWriters
  val testData = Seq(
    BlogWriters(1, "John", "Doe", "http://example.com", "2023-01-01", 5000, Seq("Campaign1")),
    BlogWriters(2, "Jane", "Smith", "http://example.org", "2023-01-02", 15000, Seq("Campaign2")),
    BlogWriters(3, "Alice", "Brown", "http://example.net", "2023-01-03", 2000, Seq("Campaign3")),
    BlogWriters(4, "Bob", "White", "http://example.edu", "2023-01-04", 30000, Seq("Campaign4"))
  )

  // make a dataset
  val blogsDS = spark.createDataset(testData)

  test("Big Hitters versus Small Hitters") {
    val bigHittersDS = blogsDS.withColumn("BigHitters", col("Hits") > 10000)

    val expectedBigHitters = Seq(
      (1, false),
      (2, true),
      (3, false),
      (4, true)
    ).toDF("Id", "BigHitters")


    val resultDS = bigHittersDS
      .select("Id", "BigHitters").as[(Int, Boolean)]

    // We are expecting the same result
    assert(resultDS.collect().sortBy(_._1) sameElements expectedBigHitters.as[(Int, Boolean)].collect().sortBy(_._1))
  }

  test("Concatenation") {

    val concatenatedDS = blogsDS
      .withColumn("AuthorsId", concat(col("Id"), col("First"), col("Last")))

    val expectedConcatenation = Seq(
      (1, "1JohnDoe"),
      (2, "2JaneSmith"),
      (3, "3AliceBrown"),
      (4, "4BobWhite")
    ).toDF("Id", "AuthorsId")

    val resultDS = concatenatedDS
      .select("Id", "AuthorsId").as[(Int, String)]

    // We are expecting the same result, with concatenation
    assert(resultDS.collect().sortBy(_._1) sameElements expectedConcatenation.as[(Int, String)].collect().sortBy(_._1))
  }

  test("Check Sort By Id") {

    // let's have a sorted DS
    val sortedDS = blogsDS
      .sort(col("Id").desc)

    val expectedSorting = Seq(
      (4, "Bob", "White"),
      (3, "Alice", "Brown"),
      (2, "Jane", "Smith"),
      (1, "John", "Doe")
    ).toDF("Id", "First", "Last")

    val resultDS = sortedDS
      .select("Id", "First", "Last").as[(Int, String, String)]

    // again, expecting to be the same
    assert(resultDS.collect() sameElements expectedSorting.as[(Int, String, String)].collect())
  }
}
