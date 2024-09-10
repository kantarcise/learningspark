package learningSpark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * This example was taken from
 * https://github.com/NeerajBhadani/spark-streaming
 * It is for to demonstrate how Watermarks work
 * with a socket stream!
 *
 * To see the effect of watermarking, before you run the Spark Application
 * run `nc -lk 9999` in the console and type:
 *
 * 2021-01-01 10:06:00#2
 * 2021-01-01 10:13:00#5
 * 2020-12-31 10:06:00#1 - (this will be discarded)
 * 2021-01-01 10:08:00#8
 * 2021-01-01 10:00:01#7
 * 2021-01-01 9:59:00#3 - (this will be discarded)
 */
object WatermarksWithSocket {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("WatermarksWithSocket")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Define host and port number to Listen.
    val host = "127.0.0.1"
    val port = "9999"

    // Make Streaming DataFrame by reading data from socket.
    val initDF: DataFrame = spark
      .readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    // Make DataFrame  with event_timestamp and val column
    val eventDF: DataFrame = initDF
      // split the data based on #
      .select(split(col("value"), "#").as("data"))
      // make a timestamp column
      .withColumn("event_timestamp", element_at(col("data"),1).cast("timestamp"))
      .withColumn("val", element_at(col("data"),2).cast("int"))
      .drop("data")

    // Without Watermarking
    // val resultDF = eventDF
    //   .groupBy(window(col("event_timestamp"), "5 minute"))
    //   .agg(sum("val").as("sum"))

    // with 10 min watermark
    val resultDF: DataFrame = eventDF
      .withWatermark("event_timestamp", "10 minutes")
      .groupBy(window(col("event_timestamp"), "5 minute"))
      .agg(sum("val").as("sum"))

    // Display DataFrame on console.
    resultDF
      .writeStream
      .outputMode("update")
      .option("truncate", value = false)
      .option("numRows", 10)
      .format("console")
      .start()
      .awaitTermination()
  }
}
