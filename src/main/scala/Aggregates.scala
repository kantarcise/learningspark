package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregates {

  // Scala unit type is used when there is
  // nothing to return.
  // It can be stated as null, no value, or nothing in return.
  def main(args: Array[String]): Unit = {

    // currently configured to run locally
    // SparkSession is the entry for all Spark Applications
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("AuthorAges")
      .getOrCreate()

    // verbose = False
    // Just so that we can get the result
    // without all the console logs
    spark.sparkContext.setLogLevel("WARN")

    // what is this?
    // https://stackoverflow.com/q/50878224
    import spark.implicits._

    // Make a DataFrame of names and ages
    val dfPeople = spark.createDataFrame(
        Seq(
          ("Brooke", 20),
          ("Brooke", 25),
          ("Denny", 31),
          ("Jules", 30),
          ("TD", 35)
        )
      )
      .toDF("name", "age")

    // there is also, toDS
    // Create a Dataset from a sequence of case class instances
    val people = Seq(
      SimplePerson("Alice", 29),
      SimplePerson("Bob", 31))

    // here is our Dataset
    val peopleDS = people
      .toDS()

    // Show the Dataset
    peopleDS.show()

    // Create a DataFrame from a sequence of tuples
    val randomDF = Seq((1, "foo"), (2, "bar"))
      .toDF("id", "value")

    // Select columns using the $ syntax
    // because we imported implicits (import spark.implicits._)
    randomDF
      .select($"id", $"value")
      .show()

    // We can group our Dataframe
    // by name, and find max age for the groups
    val maxDF = dfPeople
      .groupBy("name")
      .agg(max("age"))

    println("Max of ages on people with same name, as a group\n")
    maxDF
      .show()
    /*
     +------+--------+
     |  name|max(age)|
     +------+--------+
     |Brooke|      25|
     | Denny|      31|
     | Jules|      30|
     |    TD|      35|
     +------+--------+
     */

    // Just like max, we can use other functions.
    // Group the same names together,
    // aggregate their ages, and compute an average
    val avgDF = dfPeople
      .groupBy("name")
      .agg(avg("age"))

    // Show the results of the final execution
    avgDF.show()
    /*
    +------+--------+
    | name|avg(age)|
    +------+--------+
    |Brooke| 22.5|
    | Jules| 30.0|
    | TD| 35.0|
    | Denny| 31.0|
    +------+--------+
    */

    spark.stop()
  }
}

