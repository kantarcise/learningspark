package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, expr}

// same demonstrations, With Dataset API
object BloggersDataset {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Bloggers")
      .getOrCreate()

    // verbose = False
    spark.sparkContext.setLogLevel("WARN")

    // Define the data path as a val
    val bloggersFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/blogs.json"
    }

    // Importing implicits for Dataset
    import spark.implicits._

    // Create a Dataset by reading from the JSON file
    // We are using the case class we wrote for it!
    val blogsDS = spark
      .read
      .schema(implicitly[org.apache.spark.sql.Encoder[BlogWriters]].schema)
      .json(bloggersFilePath)
      .as[BlogWriters]

    // Show the Dataset schema as output
    println("Show Bloggers DS\n")
    blogsDS.show(truncate = false)

    // cache the DS, we will use it a lot
    blogsDS.cache()

    // Expression with expr
    println("Expression with expr\n")
    blogsDS
      .select(expr("Hits * 2").as[Int])
      .show(3)

    // Using col
    println("Using col\n")
    blogsDS
      .select((col("Hits") * 2).as[Int])
      .show(3)

    // Using the DOLLAR
    println("Using the DOLLAR\n")
    blogsDS
      .select(($"Hits" * 2).as[Int])
      .show(3)

    // Big hitters - with expr
    println("big hitters - with expr\n")
    blogsDS
      .withColumn("Big Hitters", expr("Hits > 10000"))
      .show()

    // Big hitters with col
    println("big hitters with col\n")
    blogsDS
      .withColumn("Big Hitters Bro", col("Hits") > 10000)
      .show()

    // Concatenate three columns with expr
    println("Concatenate three columns with expr\n")
    blogsDS
      .withColumn("AuthorsId", concat(expr("Id"), expr("First"), expr("Last")))
      .select(col("AuthorsId"))
      .show(5)

    // Concatenate three columns with col
    println("Concatenate three columns with col\n")
    blogsDS
      .withColumn("AuthorsId", concat(col("Id"), col("First"), col("Last")))
      .select("AuthorsId")
      .show(5)

    // Concatenate three columns with DOLLAR
    println("Concatenate three columns with DOLLAR\n")
    blogsDS
      .withColumn("AuthorsId", concat($"Id", $"First", $"Last"))
      .select($"AuthorsId")
      .show(5)

    // Sort by column Id with col
    println("sort by column Id with col\n")
    blogsDS
      .sort(col("Id").desc)
      .show()

    // Sort by column Id with DOLLAR
    println("sort by column Id with DOLLAR\n")
    blogsDS
      .sort($"Id".desc)
      .show()

    // For testing, make data in code and use it for a Dataset
    println("Just for testing, make data in code and use it for a DS")
    val authors = Seq(
      ("Matei Zaharia", "CA"),
      ("Reynold Xin", "CA")
    ).toDF("name", "state").as[(String, String)]

    println("Here is a ds made from JUST rows\n")
    authors.show()

    // Print the schema
    println("\n Here is the schema of our Bloggers DS\n")
    blogsDS.printSchema()
  }
}
