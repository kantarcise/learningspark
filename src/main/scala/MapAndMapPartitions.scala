package learningSpark

import org.apache.spark.sql.SparkSession
import scala.math.sqrt
import java.io.FileWriter

// this is taken from the books github page.
// It looks like it has been removed from the book itself.
object MapAndMapPartitions {

  // simulate a connection to a FileSystem
  def getConnection (f: String): FileWriter = {
    new FileWriter(f, true)
  }

  /*
    This method computes the square root of a given
    value, writes the result to a file, and returns
    the square root.

    This method is intended to be used with
    the map transformation.
   */
  def func(v: Long): Double = {
    // make a connection to DB
    val conn: FileWriter = getConnection("/tmp/sqrt.txt")
    val sr: Double = sqrt(v)
    // write value out to DB
    conn.write(sr.toString())
    conn.write(System.lineSeparator())
    conn.close()
    sr
  }

  /*
    This method is similar to func, but it reuses the
    provided FileWriter connection.

    This is intended to be used with the mapPartitions
    transformation to avoid opening and closing the
    connection repeatedly within each partition.

    Intended to be used with mapPartition
   */
  def funcMapPartions(conn: FileWriter, v: Long): Double = {
    val sr: Double = sqrt(v)
    conn.write(sr.toString())
    conn.write(System.lineSeparator())
    sr
  }

  /* Benchmark any code or function
  Might be useful in your apps too!
  */
  def benchmark(name: String)(f: => Unit) {
    val startTime: Long = System.nanoTime
    f
    val endTime: Long = System.nanoTime
    println(s"Time taken in $name: " +
      (endTime - startTime).toDouble / 1000000000 +
      " seconds")
  }

  def main (args: Array[String] ) {

    val spark: SparkSession = SparkSession
      .builder
      .appName("MapAndMapPartitions")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // verbose = False
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .range(1 * 10000000)
      .toDF("id")
      .withColumn("square", $"id" * $"id")
      .repartition(16)

    println("\n Here is a simple Dataframe\n")
    df.show(5)

    println("\n Benchmarking Map Function")
    // Benchmark Map function
    benchmark("map function") {
      df
        .map(r => (func(r.getLong(1))))
        .show(10)
    }

    println("\n Benchmarking Map Partition Function")
    // Benchmark MapPartition function
    benchmark("mapPartition function") {
      df
        .mapPartitions(
          iterator => {
            val conn = getConnection("/tmp/sqrt.txt")

            val result = iterator
              .map(data=>{funcMapPartions(conn, data.getLong(1))})
              .toList

            conn.close()
            result.iterator
          }
        )
        .toDF()
        .show(10)
    }

    println("\nThe mapPartitions approach is expected to be more " +
      "efficient due to fewer connections being opened and closed.")

  }

}