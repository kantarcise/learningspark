package learningSpark

import org.apache.spark.sql._

/**
 * Our goal is to predict the price per night for
 * a rental property, given our features.
 *
 * Before we get to that problem, let's solve the problem of
 * understanding the data!
 */
object AirbnbExplorePreprocessedData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Explore Preprocessed Airbnb Data")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Define the data path as a val
    val airbnbFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-airbnb-clean.parquet"
    }

    val airbnbDF = spark
      .read
      .parquet(airbnbFilePath)

    // let's see the schema
    airbnbDF
      .printSchema()

    // let's see some key columns about the data
    airbnbDF
      .select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms",
      "number_of_reviews", "price")
      .show(20, truncate = false)

    // see all the summary
    airbnbDF
      .summary()
      .show(truncate = false)

  }
}
