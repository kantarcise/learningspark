package learningSpark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.feature.Imputer

/**
 * Although this cleansing is done for us, it is
 * important that we learn the steps taken to get to the clean data,
 * so let's discover that!
 */
object AirbnbDataCleansing {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession
      .builder
      .appName("Airbnb Data Cleansing")
      .master("local[*]")
      .getOrCreate()

    // Set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    // Define the data path as a val
    val airbnbRawFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-airbnb.csv"
    }

    // Load and show raw data
    val rawDF = loadRawData(spark, airbnbRawFilePath)
    rawDF.show(20, truncate = false)
    println(s"Columns in raw DataFrame: ${rawDF.columns.mkString(", ")}")

    // Select and show base columns
    println("\nSelecting the most important columns for analysis:\n")
    val baseDF = selectBaseColumns(rawDF)
    baseDF.cache() // Cache the DataFrame for performance
    println(s"Number of rows in base DataFrame: ${baseDF.count()}")
    baseDF.show()

    // Fix price column
    val fixedPriceDF = fixPriceColumn(baseDF)
    println("\nDataFrame with 'price' column converted to DoubleType:\n")
    fixedPriceDF.show()
    println("\nSummary statistics for 'price' column:\n")
    fixedPriceDF.select("price").summary().show()

    // Drop rows with nulls in categorical features
    val noNullsDF = dropNullsInCategorical(fixedPriceDF)

    // Convert integer columns to double
    val doublesDF = convertIntegerToDouble(noNullsDF)

    // Impute missing values
    val imputedDF = imputeMissingValues(doublesDF)
    println("\nDataFrame after imputing missing values:\n")
    imputedDF.show()

    // Filter data with extreme values
    val cleanDF = filterExtremeValues(imputedDF)
    println("\nHere is our cleansed DataFrame:\n")
    cleanDF.show(truncate = false)

    // Save the cleansed data
    val outputPath = "/tmp/sf-airbnb/sf-airbnb-clean.parquet"
    saveCleanedData(cleanDF, outputPath)

    // Stop the SparkSession
    spark.stop()
  }

  /**
   * Load raw data from CSV file.
   *
   * @param spark SparkSession
   * @param path  File path to the CSV file
   * @return DataFrame containing the raw data
   */
  def loadRawData(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")       // File has a header row
      .option("multiLine", "true")    // Handle multi-line entries
      .option("inferSchema", "true")  // Infer data types
      .option("escape", "\"")         // Handle escaped quotes
      .csv(path)
  }

  /**
   * Select base columns for further processing.
   *
   * @param df DataFrame containing raw data
   * @return DataFrame with selected columns
   */
  def selectBaseColumns(df: DataFrame): DataFrame = {
    df.select(
      "host_is_superhost",
      "cancellation_policy",
      "instant_bookable",
      "host_total_listings_count",
      "neighbourhood_cleansed",
      "latitude",
      "longitude",
      "property_type",
      "room_type",
      "accommodates",
      "bathrooms",
      "bedrooms",
      "beds",
      "bed_type",
      "minimum_nights",
      "number_of_reviews",
      "review_scores_rating",
      "review_scores_accuracy",
      "review_scores_cleanliness",
      "review_scores_checkin",
      "review_scores_communication",
      "review_scores_location",
      "review_scores_value",
      "price"
    )
  }

  /**
   * Fix the price column by removing "$" and "," and converting to double.
   *
   * @param df DataFrame with raw 'price' column
   * @return DataFrame with 'price' column cleaned and converted to DoubleType
   */
  def fixPriceColumn(df: DataFrame): DataFrame = {
    df.withColumn("price", translate(col("price"), "$,", "").cast("double"))
  }

  /**
   * Drop rows with null values in categorical features.
   *
   *
   * There are a lot of different ways to handle null values.
   * Sometimes, null can actually be a key indicator of the thing
   * you are trying to predict (e.g. if you don't fill in certain
   * portions of a form, probability of it getting approved decreases).
   *
   * Some ways to handle nulls:
   *
   * Drop any records that contain nulls.
   * Numeric:
   *    Replace them with mean/median/zero/etc.
   * Categorical:
   *    Replace them with the mode
   *    Make a special category for null
   * Use techniques like ALS which are designed to impute missing values
   *
   * If we do ANY imputation techniques for categorical/numerical
   * features, we MUST include an additional field specifying that field
   * was imputed (think about why this is necessary)
   *
   * @param df DataFrame
   * @return DataFrame with no nulls in specified categorical columns
   */
  def dropNullsInCategorical(df: DataFrame): DataFrame = {
    println("\nDropping rows with null values in 'host_is_superhost' column.\n")
    df.na.drop(cols = Seq("host_is_superhost"))
  }

  /**
   * Convert integer columns to double.
   *
   * Spark MLlib's Imputer requires all input columns to be of DoubleType.
   * https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/feature/Imputer.html.
   *
   * This function casts all IntegerType columns to DoubleType.
   *
   * @param df DataFrame
   * @return DataFrame with integer columns converted to double
   */
  def convertIntegerToDouble(df: DataFrame): DataFrame = {
    // Detect all integer columns
    val integerColumns = df.schema.fields.collect {
      case field if field.dataType == IntegerType => field.name
    }

    // Cast all integer columns to double
    val doublesDF = integerColumns.foldLeft(df) { (tempDF, colName) =>
      tempDF.withColumn(colName, col(colName).cast("double"))
    }

    val columns = integerColumns.mkString("\n - ")
    println(s"\nColumns converted from IntegerType to DoubleType:\n - $columns \n")
    println("*" * 80)
    doublesDF
  }

  /**
   * Impute missing values in specified columns.
   *
   * This function uses the median strategy to impute missing values in numerical columns.
   * It also adds indicator columns to flag where imputation has occurred.
   *
   * @param df DataFrame
   * @return DataFrame with imputed values and indicator columns
   */
  def imputeMissingValues(df: DataFrame): DataFrame = {
    // Columns to impute
    val imputeCols = Array(
      "bedrooms",
      "bathrooms",
      "beds",
      "review_scores_rating",
      "review_scores_accuracy",
      "review_scores_cleanliness",
      "review_scores_checkin",
      "review_scores_communication",
      "review_scores_location",
      "review_scores_value"
    )

    /* We can use a for loop:
     var tempDF = df
     for (c <- imputeCols) {
       tempDF = tempDF.withColumn(c + "_na",
         when(col(c).isNull, 1.0)
           .otherwise(0.0))
    }
     */

    // We can use foldLeft
    // foldLeft is better than a for loop because it enables
    // immutable, functional transformations without mutable
    // state, resulting in more concise, readable, and less error-prone code.
    // Add indicator columns for missing values
    val dfWithIndicators = imputeCols.foldLeft(df) { (tempDF, colName) =>
      tempDF.withColumn(s"${colName}_na", when(col(colName).isNull, 1.0).otherwise(0.0))
    }

    println("\nDataFrame with indicator columns for missing values:\n")
    dfWithIndicators.show()

    // Impute missing values using median strategy
    val imputer = new Imputer()
      .setStrategy("median")
      .setInputCols(imputeCols)
      .setOutputCols(imputeCols)

    imputer.fit(dfWithIndicators).transform(dfWithIndicators)
  }

  /**
   * Filter rows with extreme values and clean data.
   *
   * This function removes listings with non-positive
   * prices and excessively long minimum nights.
   *
   * @param df DataFrame
   * @return Cleaned DataFrame
   */
  def filterExtremeValues(df: DataFrame): DataFrame = {
    println("\nSummary statistics for 'price' column before filtering:\n")
    df.select(col("price")).summary().show()

    println("\nFiltering out listings with non-positive prices and excessive minimum nights.\n")

    df.filter(col("price") > 0)
      .filter(col("minimum_nights") <= 365)
  }

  /**
   * Save the cleaned DataFrame to a Parquet file.
   *
   * @param df   DataFrame to save
   * @param path Output file path
   */
  def saveCleanedData(df: DataFrame, path: String): Unit = {
    df.write.mode("overwrite").parquet(path)
    println(s"\nSaved the cleaned data to $path")
  }
}
