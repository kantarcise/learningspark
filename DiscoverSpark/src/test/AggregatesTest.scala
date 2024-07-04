package learningSpark

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

class AggregatesTest extends AnyFunSuite{

  // assuming a local setup
  private val spark = SparkSession.builder()
    .appName("OrderProcessTesting")
    .master("local[*]")
    .getOrCreate()

  // verbose = False
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  test("Simple Aggregation Test"){
    val testDF = Seq(
      SimplePerson("Mark", 42),
      SimplePerson("Neil", 38)
    ).toDF()

    // Perform aggregation: Calculate the average age
    val avgAgeDf = testDF
        .agg(avg("age")
        .as("average_age"))

    // Collect the result (head is the first element)
    val result = avgAgeDf.collect().head.getDouble(0)

    // Assert the result is as expected
    assert(result == 40)

  }

  test("Dataframe Understanding"){
    val testDF = Seq(
      SimplePerson("Mark", 42),
      SimplePerson("Neil", 38)
    ).toDF()

    // Check the schema
    val schema = testDF.schema
    assert(schema.fieldNames.sameElements(Array("name", "age")))

    // Check the first row
    val firstRow = testDF.head()
    assert(firstRow.getAs[String]("name") == "Mark")
    assert(firstRow.getAs[Int]("age") == 42)
  }


  test("Dataframe to Datasets"){
    val testDF = Seq(
      SimplePerson("Mark", 42),
      SimplePerson("Neil", 38)
    ).toDF()

    // Convert DataFrame to Dataset
    val testDS = testDF.as[SimplePerson]

    // Verify the conversion
    assert(testDS.isInstanceOf[Dataset[SimplePerson]])

    // Convert back to DataFrame
    val backToDf = testDS.toDF()

    // Verify the data integrity
    val collectedData = backToDf.collect()
    assert(collectedData.length == 2)
    assert(collectedData.contains(Row("Mark", 42)))
    assert(collectedData.contains(Row("Neil", 38)))
  }

}
