package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

/**
 * Same demonstrations, With Dataset API
 *
 * We are using a case class BlogWriters and getting
 * the schema thanks to it!
 *
 * We are also using a scoped case class Authors just to
 * demonstrate the usage for toDS()
 */
object BloggersDataset {

  case class Authors(name: String, state: String)

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

    import spark.implicits._

    // Make a Dataset by reading from the JSON file
    // We are using the case class we wrote for it!
    val blogsDS = spark
      .read
      .schema(implicitly[org.apache.spark.sql.Encoder[BlogWriters]].schema)
      .json(bloggersFilePath)
      .as[BlogWriters]

    // Show the Dataset schema as output
    println("\nShow Bloggers DS\n")
    blogsDS
      .show(truncate = false)

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
    println("Big hitters - with expr\n")
    blogsDS
      .withColumn("Big Hitters", expr("Hits > 10000"))
      .show()

    // Sort by column Id with col
    // sort is a Typed Transformation!
    // https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html
    println("Sort by column Id with col\n")
    blogsDS
      .sort(col("Id").desc)
      .show()

    // Sort by column Id with DOLLAR
    println("sort by column Id with DOLLAR\n")
    blogsDS
      .sort($"Id".desc)
      .show()

    // toDF was to convert strongly typed
    // collections to Dataframes

    // If we want to use toDS, we should have a case class for Authors
    // and make our sequence with them!
    // For testing, make data in code and use it for a Dataset
    println("Write some data in code and use it for a DS\n")
    val authors = Seq(
      Authors("Matei Zaharia", "CA"),
      Authors("Reynold Xin", "CA"))
      .toDS()

    println("Here is a ds made with toDS(), from Authors objects\n")
    authors.show()

    // Print the schema
    println("\n Here is the schema of our Bloggers DS\n")
    blogsDS.printSchema()
  }
}
