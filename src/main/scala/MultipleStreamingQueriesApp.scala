package learningSpark

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Let's see how we can monitor a Spark Streaming Application
 * with Prometheus and Grafana.
 */
object MultipleStreamingQueriesApp {

  case class Weather(last_updated: String,
                     last_updated_epoch: Int,
                     temp_c: Double,
                     temp_f: Double,
                     feelslike_c: Double,
                     feelslike_f: Double,
                     windchill_c: Double,
                     windchill_f: Double,
                     heatindex_c: Double,
                     heatindex_f: Double,
                     dewpoint_c: Double,
                     dewpoint_f: Double,
                     condition_text: String,
                     condition_icon: String,
                     condition_code: Int)

  case class Wind(wind_mph: Double,
                  wind_kph: Double,
                  wind_degree: Int,
                  wind_dir: String,
                  pressure_mb: Double,
                  pressure_in: Double,
                  precip_mm: Double,
                  precip_in: Double,
                  humidity: Int,
                  cloud: Int,
                  is_day: Int,
                  uv: Double,
                  gust_mph: Double,
                  gust_kph: Double)

  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession
      .builder
      .appName("MultipleStreamingQueriesApp")
      .config("spark.ui.prometheus.enabled", "true")
      .config("spark.sql.streaming.metricsEnabled", "true")
      .config("spark.executor.processTreeMetrics.enabled", "true")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Define the rate sources to simulate weather and wind streams
    val weatherStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      .selectExpr(
        "CAST(timestamp AS STRING) AS last_updated",
        "CAST(unix_timestamp() AS INT) AS last_updated_epoch",
        "CAST(rand() * 40 - 10 AS DOUBLE) AS temp_c",
        "CAST(rand() * 100 AS DOUBLE) AS temp_f",
        "CAST(rand() * 40 - 10 AS DOUBLE) AS feelslike_c",
        "CAST(rand() * 100 AS DOUBLE) AS feelslike_f",
        "CAST(rand() * 40 - 10 AS DOUBLE) AS windchill_c",
        "CAST(rand() * 100 AS DOUBLE) AS windchill_f",
        "CAST(rand() * 40 - 10 AS DOUBLE) AS heatindex_c",
        "CAST(rand() * 100 AS DOUBLE) AS heatindex_f",
        "CAST(rand() * 40 - 10 AS DOUBLE) AS dewpoint_c",
        "CAST(rand() * 100 AS DOUBLE) AS dewpoint_f",
        "'Sunny' AS condition_text",
        "'sunny_icon.png' AS condition_icon",
        "101 AS condition_code"
      ).as[Weather]

    val windStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      .selectExpr(
        "CAST(rand() * 50 AS DOUBLE) AS wind_mph",
        "CAST(rand() * 100 AS DOUBLE) AS wind_kph",
        "CAST(rand() * 360 AS INT) AS wind_degree",
        "'N' AS wind_dir",
        "CAST(rand() * 1000 AS DOUBLE) AS pressure_mb",
        "CAST(rand() * 40 AS DOUBLE) AS pressure_in",
        "CAST(rand() * 100 AS DOUBLE) AS precip_mm",
        "CAST(rand() * 40 AS DOUBLE) AS precip_in",
        "CAST(rand() * 100 AS INT) AS humidity",
        "CAST(rand() * 100 AS INT) AS cloud",
        "CAST(rand() * 2 AS INT) AS is_day",
        "CAST(rand() * 10 AS DOUBLE) AS uv",
        "CAST(rand() * 100 AS DOUBLE) AS gust_mph",
        "CAST(rand() * 200 AS DOUBLE) AS gust_kph"
      ).as[Wind]

    // Run weather streaming query
    val weatherQuery = weatherStream.writeStream
      .queryName("WeatherQuery")
      .format("console")
      .outputMode("append")
      .start()

    // Run wind streaming query
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool2")
    val windQuery = windStream.writeStream
      .queryName("WindQuery")
      .format("console")
      .outputMode("append")
      .start()

    // Await termination
    weatherQuery.awaitTermination()
    windQuery.awaitTermination()
  }
}
