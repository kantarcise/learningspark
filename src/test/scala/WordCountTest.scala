package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

case class WordCount(value: String, count: Long)

class WordCountTest extends AnyFunSuite with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("WordCountTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // if more than one test, these might be handy!
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("WordCount should count words correctly") {
    // Implicitly provide the required SQLContext
    implicit val sqlContext = spark.sqlContext

    // let's use MemoryStream for testing
    val inputStream = MemoryStream[String]

    val words: Dataset[String] = inputStream
      .toDS()
      .flatMap(_.split(" "))

    val wordCounts: Dataset[WordCount] = words
      .groupByKey(identity)
      .count()
      .map { case (word, count) => WordCount(word, count) }

    val query: StreamingQuery = wordCounts
      .writeStream
      .format("memory")
      .queryName("wordCounts")
      .outputMode(OutputMode.Complete())
      .start()

    inputStream.addData("hello world", "hello Spark")

    query.processAllAvailable()

    val result = spark
      .sql("select * from wordCounts")
      .as[WordCount]
      .collect()

    assert(result.contains(WordCount("hello", 2)))
    assert(result.contains(WordCount("world", 1)))
    assert(result.contains(WordCount("Spark", 1)))

    query.stop()
  }
}
