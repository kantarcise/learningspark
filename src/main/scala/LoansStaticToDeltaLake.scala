package learningSpark

import org.apache.spark.sql.SparkSession

/**
 * Let's load a static parquet file into a
 * Delta Lake table to get started!
 */
object LoansStaticToDeltaLake {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Load Static Data To Delta Late")
      .master("local[*]")
      // we have to configure our SparkSession with
      // the Extension and Catalog
      .config("spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Define the data path as a val
    val loanRiskFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/loan-risks.snappy.parquet"
    }

    // Configure Delta Lake path
    val deltaPath = "/tmp/loans_delta"

    // you can combine a read & write query!
    spark
      .read
      .format("parquet")
      .load(loanRiskFilePath)
      .write
      // overwrite so that
      // it doesn't error on rerun
      .mode("overwrite")
      .format("delta")
      .save(deltaPath)

    // let's see the schema
    spark
      .read
      .format("parquet")
      .load(loanRiskFilePath)
      .printSchema()

    // Make a view on the data called loans_delta
    spark
      .read
      .format("delta")
      .load(deltaPath)
      .createOrReplaceTempView("loans_delta")

    // now we can read and explore the data
    spark
      .sql("SELECT count(*) FROM loans_delta")
      .show()

    // let's see first 5 rows
    spark
      .sql("SELECT * FROM loans_delta LIMIT 5")
      .show()

  }
}
