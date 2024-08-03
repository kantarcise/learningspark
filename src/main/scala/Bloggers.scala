package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, expr}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

object Bloggers {

  // we can define a schema ourselves!
  val authorSchema = StructType(Seq(
    StructField("name", StringType, nullable = true),
    StructField("state", StringType, nullable = true)
  ))

  def main(args: Array[String]) {

    // builder VS builder()
    // In Scala, if a method does not take any
    //  parameters, you can call it with or without parentheses.
    // Here is more info:
    // https://docs.scala-lang.org/style/method-invocation.html
    val spark = SparkSession
      .builder
      //  A master URL must be set in your configuration
      // we are continuing with local mode!
      .master("local[*]")
      .appName("Bloggers")
      .getOrCreate()

    // verbose = False
    spark.sparkContext.setLogLevel("WARN")

    // we know that we can get the filepath as argument
    /*
    if (args.length <= 0) {
      println("usage Example3_7 <file path to blogs.json>")
      System.exit(1)
    }
    // Get the path to the JSON file
    val jsonFile = args(0)
    */

    // Define the data path as a val
    val bloggersFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/blogs.json"
    }

    // Let's define our schema programmatically
    val schema = StructType(Array(
      StructField("Id", IntegerType, nullable = false),
      StructField("First", StringType, nullable = false),
      StructField("Last", StringType, nullable = false),
      StructField("Url", StringType, nullable = false),
      StructField("Published", StringType, nullable = false),
      StructField("Hits", IntegerType, nullable = false),
      StructField("Campaigns", ArrayType(StringType), nullable = false))
    )

    // Make a DataFrame by reading from the JSON file
    // with a predefined schema
    val blogsDF = spark
      .read
      .schema(schema)
      .json(bloggersFilePath)

    // We can also do this -
    //  the unified way => read - format - load
    // val blogsDFSecond = spark
    //   .read
    //   .schema(schema)
    //   .format("json")
    //   .load(bloggersFilePath)

    // Show the DataFrame schema as output
    println("\nShow Bloggers DF\n")
    blogsDF.show(truncate = false)

    // Let's cache the DF, we will use it a lot
    blogsDF.cache()

    // just prints Id
    // println(blogsDF.col("Id"))

    import spark.implicits._

    // These statements return the same value, showing that
    // expr is the same as a col method call

    // calculate an expression with expr
    println("Same statement in First Approach - Expression with expr\n")
    blogsDF
      .select(expr("Hits * 2"))
      .show(3)

    println("Same statement in Second Approach - Using col\n")
    // or use col
    blogsDF
      .select(col("Hits") * 2)
      .show(3)

    println("We can just use quotation marks, but no calculation here\n")
    // or just use quotation marks - the code down below
    // works, which is kinda amazing
    // if you want expressions in select, use Dollar Or Col
    blogsDF
      .select("Hi" + "ts")
      .show(3)

    println("Same statement in Fourth Approach - Using the DOLLAR\n")
    // Can we use the DOLLAR ? Yes
    blogsDF
      .select($"Hits" * 2)
      .show(3)

    println("\nLet's now calculate the big hitters, articles that got the most clicks\n")

    // big hitters - with expr
    println("Big hitters - with expr\n")
    blogsDF
      .withColumn("BigHitters", expr("Hits > 10000"))
      .show()

    println("Big hitters - with col\n")
    // big hitters with col
    blogsDF
      .withColumn("BigHittersHere", col("Hits") > 10000)
      .show()

    println("\nLet's learn how to Concatenate Columns.\n")

    // Concatenate three columns, make a new column, and show the
    // newly made concatenated column

    println("Concatenate three columns with expr\n")
    blogsDF
      .withColumn("AuthorsId", concat(expr("Id"), expr("First"), expr("Last")))
      .select(col("AuthorsId"))
      .show(5)

    println("Concatenate three columns with col\n")
    // can you do it with col ?
    blogsDF
      .withColumn("AuthorSID", concat(col("Id"), col("First"), col("Last")))
      .select("AuthorSID")
      .show(5)

    println("Concatenate three columns with DOLLAR\n")
    // can you do it with dollar?
    blogsDF
      .withColumn("AuthorSID", concat($"Id", $"First", $"Last"))
      .select($"AuthorSID")
      .show(5)

    println("\nWe can sort the Dataframe by some columns\n")

    println("Sort by column Id with col\n")
    // Sort by column "Id" in descending order
    blogsDF
      .sort(col("Id").desc)
      .show()

    println("Sort by column Id with DOLLAR\n")
    blogsDF
      .sort($"Id".desc)
      .show()

    // What really is this $ usage ?
    // It is from spark framework, which represents a column.
    // Here is more:  - https://stackoverflow.com/a/42427471

    /*
    In this last example, the expressions blogs_df.sort(col("Id").desc) and
      blogsDF.sort($"Id".desc) are identical. They both sort the DataFrame column
      named Id in descending order: one uses an explicit function, col("Id"), to return a
      Column object, while the other uses $ before the name of the column, which is a
      function in Spark that converts column named Id to a Column
     */

    println("\nJust for testing, make data in code and use it for a DF:\n")

    // We can make a Dataframe from just using Rows
    val rows = Seq(
      Row("Matei Zaharia", "CA"),
      Row("Reynold Xin", "CA")
    )

    val authorsDF = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      authorSchema)

    println("Here is a df made from JUST rows\n")
    authorsDF.show()

    println("We can also use toDF() to make Dataframes\n")

    val authorsDFSecond = Seq(
      ("Matei Zaharia", "CA"),
      ("Reynold Xin", "CA"))
      .toDF("name", "state")

    authorsDFSecond.show()

    // We can also print the schema
    println("\n Here is the schema of our Bloggers DF\n")
    blogsDF.printSchema
  }
}
