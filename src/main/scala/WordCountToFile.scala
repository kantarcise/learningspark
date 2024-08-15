package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}

/** Let's count some words again,
 * but this time, write the results into a file!
 *
 * Thanks to foreachBatch, we can write into multiple files.
 */
object WordCountToFile {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .appName("WordCountToCassandra")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Make a dataset representing the stream
    // of input lines from connection to localhost:9999
    val linesDS: Dataset[String] = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
      .as[String]

    // Split the lines into words
    val words: Dataset[String] = linesDS
      .flatMap(_.split(" "))

    // Perform the word count using Dataset API
    val wordCounts: Dataset[WordCount] = words
      .groupByKey(identity)
      .count()
      .map { case (word, count) => WordCount(word, count) }

    // Define the paths where you want to save the data
    val pathOne = "/tmp/spark_output/path_one"
    val pathTwo = "/tmp/spark_output_2/path_two"

    // Write the word counts to Cassandra using foreachBatch
    val query = wordCounts
      .writeStream
      .queryName("Write into Multiple Files")
      // we are using the defined method for foreachBatch
      .foreachBatch{{ (batchDS: Dataset[WordCount], batchId: Long) =>
        writeCountsToMultipleLocations(batchDS, // batchId,
          pathOne, pathTwo, writeToSingleFile = true)
      }}
      // Use "complete" mode to keep the running count
      .outputMode("complete")
      .option("checkpointLocation", "/tmp/spark_checkpoint_4")
      // .trigger(Trigger.ProcessingTime("3 seconds"))
      .start()

    query.awaitTermination()
  }

  /**
   * With foreach batch, we can also write to multiple locations
   * Let's see how we can save our streaming Dataset into two json files.
   *
   * This method works such that we get a different json file
   * for each word we type.
   *
   * If we want all the words to be in a single file,
   * we use writeToSingleFile.
   *
   * @param wordCountsDS: the streaming dataset micro batch
   * @param pathOne: first file path to be written into
   * @param pathTwo: second file path to be written into
   * @param writeToSingleFile: whether or not writing into single file.
   * */
  def writeCountsToMultipleLocations(wordCountsDS: Dataset[WordCount],
                                     // batchId: Long,
                                     pathOne: String,
                                     pathTwo: String,
                                     writeToSingleFile: Boolean) {

    // you can see the whole batch! A single sentence before you press enter!
    // wordCountsDS.show()

    // idea taken from:
    // https://docs.databricks.com/en/structured-streaming/foreach.html
    if (!wordCountsDS.isEmpty) {

      // Determine the appropriate Dataset for writing
      val dsToWrite = if (writeToSingleFile) wordCountsDS.coalesce(1) else wordCountsDS

      // To avoid re-computations, we should cache the Dataset,
      // write it to multiple locations, and then uncache it:
      dsToWrite.persist()

      // without append mode, we would errorifexists (default).
      // 4 choices - append / overwrite / error / ignore

      // Location 1
      dsToWrite
        // If you want a single file output
        .write
        .format("json")
        .mode("overwrite")
        .save(pathOne)

      // Location 2
      dsToWrite
        // If you want a single file output
        .write
        .format("json")
        .mode("overwrite")
        .save(pathTwo)

      dsToWrite.unpersist()
    }
  }
}