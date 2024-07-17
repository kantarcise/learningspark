package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.funsuite.AnyFunSuite


class WordCountToFileTest extends AnyFunSuite {

  test("Word count streaming job writes correct output") {
    val spark = SparkSession
      .builder
      .appName("WordCountToFileTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // Create MemoryStream to simulate input stream
    val linesStream = MemoryStream[String](1, spark.sqlContext)
    val linesDS: Dataset[String] = linesStream.toDS()

    // Split the lines into words
    val words: Dataset[String] = linesDS.flatMap(_.split(" "))

    // Perform the word count using Dataset API
    val wordCounts: Dataset[WordCount] = words
      .groupByKey(identity)
      .count()
      .map { case (word, count) => WordCount(word, count) }

    // Define the paths where you want to save the data
    val pathOne = "/tmp/spark_output_test/path_one"
    val pathTwo = "/tmp/spark_output_test_2/path_two"

    // Start the query with foreachBatch
    val query = wordCounts.writeStream
      .foreachBatch { (batchDS: Dataset[WordCount], batchId: Long) =>
        WordCountToFile.writeCountsToMultipleLocations(batchDS, batchId,
          pathOne, pathTwo, writeToSingleFile = true)
      }
      .outputMode(OutputMode.Complete())
      .option("checkpointLocation", "/tmp/spark_checkpoint_test")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Add data to the MemoryStream
    linesStream.addData("test test test")
    query.processAllAvailable()

    // Validate the output
    val resultPathOne = spark.read.json(pathOne).as[WordCount]
    val resultPathTwo = spark.read.json(pathTwo).as[WordCount]

    // Validate that the data is written correctly to both paths
    assert(resultPathOne.count() == 1)
    assert(resultPathTwo.count() == 1)
    assert(resultPathOne.collect().contains(WordCount("test", 3)))
    assert(resultPathTwo.collect().contains(WordCount("test", 3)))

    // Stop the query
    query.stop()

    // Cleanup
    spark.close()
  }
}
