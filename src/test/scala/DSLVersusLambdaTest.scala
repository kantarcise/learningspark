package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

import java.util.Calendar

class DSLVersusLambdaTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("DSLVersusLambdaTest")
    .getOrCreate()

  import spark.implicits._

  // Sample data
  val sampleData = Seq(
    Person(2303, "Doe", "Vicky", "Izod", "Female", "05/28/1981", "2237961824", "322160"),
    Person(4986, "Martainn", "Alejandro", "Rouf", "Male", "05/26/1972", "5392345271", "253334"),
    Person(3381, "Betteann", "Colline", "Croci", "Genderfluid", "06/12/1992", "4053116996", "264876"),
    Person(6142, "Derby", "Gian", "Bettley", "Male", "09/21/1992", "4879124362", "148951"),
    Person(1234, "Dylan", "John", "James", "Male", "03/15/1975", "1234567890", "90000")
  )

  val personDS = spark.createDataset(sampleData)

  test("Test date_format and to_date methods") {
    // Adjusted based on the year 2023
    val earliestYear = Calendar.getInstance.get(Calendar.YEAR) - 40

    val resultDS = personDS
      .filter(date_format(to_date($"birthDate", "MM/dd/yyyy"), "yyyy").cast("int") < earliestYear) // Everyone above 40
      .filter($"salary".cast("int") > 80000) // Everyone earning more than 80K
      .filter($"lastName".startsWith("J")) // Last name starts with J
      .filter($"firstName".startsWith("D")) // First name starts with D

    val expectedData = Seq(
      Person(1234, "Dylan", "John", "James", "Male", "03/15/1975", "1234567890", "90000")
    )

    val expectedDS = spark.createDataset(expectedData)

    // collects are iterables
    // and they have a sameElements method!
    assert(resultDS.collect().sameElements(expectedDS.collect()))

    // or we can use a method we defined
    // for this comparison!
    assert(compareDatasets(resultDS, expectedDS))
  }

  def compareDatasets[T](ds1: Dataset[T], ds2: Dataset[T]): Boolean = {
    val df1 = ds1.toDF()
    val df2 = ds2.toDF()

    val diff1 = df1.except(df2)
    val diff2 = df2.except(df1)

    diff1.isEmpty && diff2.isEmpty
  }

}