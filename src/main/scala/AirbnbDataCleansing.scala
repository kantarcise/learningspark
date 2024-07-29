package learningSpark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, translate, when}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.feature.Imputer

/**
 * Although this cleansing is done for us, it is
 * important that we learn the steps taken to get to the clean data,
 * so let's discover that!
 */
object AirbnbDataCleansing {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Airbnb Data Cleansing")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Define the data path as a val
    val airbnbRawFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-airbnb.csv"
    }

    // Load and show raw data
    val rawDF = loadRawData(spark, airbnbRawFilePath)
    rawDF.show(20, truncate = false)
    println(rawDF.columns.mkString("Array(", ", ", ")"))

    // Select and show base columns
    println("Arguably, most important columns selected:\n")
    val baseDF = selectBaseColumns(rawDF)
    baseDF.cache().count()
    baseDF.show()

    // Fix price column
    val fixedPriceDF = fixPriceColumn(baseDF)

    println("Here is the Dataframe with price in DoubleType: \n")
    fixedPriceDF.show()

    println("Here is the summary for it:\n")
    fixedPriceDF.summary().show()

    // Drop rows with nulls in categorical features
    val noNullsDF = dropNullsInCategorical(fixedPriceDF)

    // Convert integer columns to double
    val doublesDF = convertIntegerToDouble(noNullsDF, baseDF)

    // Impute missing values
    // https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/feature/Imputer.html
    val imputedDF = imputeMissingValues(doublesDF)

    // Filter data with extreme values
    val cleanDF = filterExtremeValues(imputedDF)

    println("Here is our cleansed dataframe: \n")
    cleanDF.show(truncate = false)

    // Save the cleansed data
    val outputPath = "/tmp/sf-airbnb/sf-airbnb-clean.parquet"
    saveCleanedData(cleanDF, outputPath)
  }

  /**
   * Load raw data from CSV file.
   * @param spark SparkSession
   * @param path File path
   * @return DataFrame
   */
  def loadRawData(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .option("inferSchema", "true")
      .option("escape", "\"")
      .csv(path)
  }

  /**
   * Select base columns for further processing.
   * @param df DataFrame
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
      "price")
  }

  /**
   * Fix the price column by removing "$" and "," and converting to double.
   * @param df DataFrame
   * @return DataFrame with fixed price column
   */
  def fixPriceColumn(df: DataFrame): DataFrame = {
    df
      .withColumn("price", translate(col("price"), "$,", "").cast("double"))
  }

  /**
   * Drop rows with null values in categorical features.
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
   * @return DataFrame with no nulls in categorical features
   */
  def dropNullsInCategorical(df: DataFrame): DataFrame = {
    println("Dropping Null values in host_is_superhost column.\n")
    df.na.drop(cols = Seq("host_is_superhost"))
  }

  /**
   * Convert integer columns to double.
   *
   * SparkML's `Imputer` requires all fields be of type double
   * https://spark.apache.org/docs/latest/api/scala/org/apache/spark/ml/feature/Imputer.html.

   * Let's cast all integer fields to double.
   *
   * @param df DataFrame
   * @param baseDF DataFrame with original schema
   * @return DataFrame with integer columns converted to double
   */
  def convertIntegerToDouble(df: DataFrame,
                             baseDF: DataFrame): DataFrame = {
    // detect all integer columns
    val integerColumns = for (x <- baseDF.schema.fields if x.dataType == IntegerType) yield x.name
    var doublesDF = df
      // cast all the columns to double
      for (c <- integerColumns) {
         doublesDF = doublesDF.withColumn(c, col(c).cast("double"))
    }
    val columns = integerColumns.mkString("\n - ")
    println(s"Columns converted from Integer to Double:\n - $columns \n")
    println("*-"*80)
    doublesDF
  }

  /**
   * Impute missing values in specified columns.
   *
   * After we are imputing the median value, we add
   * an indicator column.
   *
   * na - not available
   * https://www.statmethods.net/data-input/missingdata.html
   *
   * This way the ML model or human analyst can interpret
   * any value in that column as an imputed value, not a true value.
   *
   * @param df DataFrame
   * @return DataFrame with imputed values
   */
  def imputeMissingValues(df: DataFrame): DataFrame = {
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

    var tempDF = df
    for (c <- imputeCols) {
      tempDF = tempDF.withColumn(c + "_na",
        when(col(c).isNull, 1.0)
          .otherwise(0.0))
    }

    println("\nHere is a Imputed Dataframe :\n")
    tempDF.show()

    val imputer = new Imputer()
      .setStrategy("median")
      .setInputCols(imputeCols)
      .setOutputCols(imputeCols)

    imputer.fit(tempDF).transform(tempDF)
  }

  /**
   * Filter rows with extreme values and clean data.
   * @param df DataFrame
   * @return Cleaned DataFrame
   */
  def filterExtremeValues(df: DataFrame): DataFrame = {
    println("Here is price column summary: \n")
    df.select(col("price")).summary().show()

    println("Now filtering extreme values: \n")

    df.filter(col("price") > 0)
      .filter(col("minimum_nights") <= 365)
  }

  /**
   * Save the cleaned DataFrame to a file.
   * @param df DataFrame
   * @param path Output file path
   */
  def saveCleanedData(df: DataFrame,
                      path: String): Unit = {
    df.write.mode("overwrite").parquet(path)
    println(s"Saved the model to $path")
  }
}
