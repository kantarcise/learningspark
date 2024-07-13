package learningSpark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast

/*
  Join operations are a common type of transformation in big data analytics in which
  two data sets, in the form of tables or DataFrames, are merged over a common
  matching key.

  Similar to relational databases, the Spark DataFrame and Dataset APIs
  and Spark SQL offer a series of join transformations: inner joins, outer joins, left
  joins, right joins, etc.

  All of these operations trigger a large amount of data movement across Spark executors.
 */
object BroadcastHashJoin {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .appName("Joins")
      .master("local[*]")
      .getOrCreate()

    // verbose = False
    spark.sparkContext.setLogLevel("ERROR")

    // A common use case is when you have a common set of keys between two DataFrames,
    // one holding less information than the other, and you need a merged view of both.

    // For example, consider a simple case where you have a large data set of soccer
    // players around the world, playersDF, and a smaller data set of soccer clubs they
    // play for, clubsDF, and you wish to join them over a common key:

    // Scala can return multiple values from a single method.
    val (playersDF: DataFrame, clubsDF: DataFrame) = getDataframes(spark)

    // In this code we are forcing Spark to do a broadcast join, but it will
    //  resort to this type of join by default if the size of the smaller data set
    //  is below the spark.sql.autoBroadcastJoinThreshold
    val joinedDF = playersDF.join(broadcast(clubsDF), "club_name")

    // see physical plan
    // modes are 'simple', 'extended', 'codegen', 'cost', 'formatted'.
    joinedDF.explain(true)

    // Show the result of the join operation
    println("This is the result of broadcast hash join\n")
    joinedDF.show()

    // lets see the size of clubsDF
    getSizeOfDataframe(spark, clubsDF)

    // You can also see the size on storage tab in UI
    // 6.5 KiB approximately
    Thread.sleep(1000* 30 * 1)

  }

  /*
    Return two dataframes that are made from just Sequences.
   */
  def getDataframes(spark: SparkSession):(DataFrame, DataFrame) = {
    import spark.implicits._

    val players: DataFrame = Seq(
      (1, "Lionel Messi", "FC Barcelona"),
      (2, "Cristiano Ronaldo", "Juventus"),
      (3, "Neymar Jr", "Paris Saint-Germain"),
      (4, "Luka Modric", "Real Madrid"),
      (5, "Kylian Mbappe", "Paris Saint-Germain"),
      (6, "Sergio Ramos", "Real Madrid"),
      (7, "Kevin De Bruyne", "Manchester City"),
      (8, "Robert Lewandowski", "Bayern Munich"),
      (9, "Virgil van Dijk", "Liverpool"),
      (10, "Harry Kane", "Tottenham Hotspur"),
      (11, "Eden Hazard", "Real Madrid"),
      (12, "Sadio Mane", "Liverpool"),
      (13, "Raheem Sterling", "Manchester City"),
      (14, "Mohamed Salah", "Liverpool"),
      (15, "Alisson Becker", "Liverpool"),
      (16, "Marc-Andre ter Stegen", "FC Barcelona"),
      (17, "Jan Oblak", "Atletico Madrid"),
      (18, "Gianluigi Donnarumma", "AC Milan"),
      (19, "Erling Haaland", "Borussia Dortmund"),
      (20, "Romelu Lukaku", "Inter Milan"),
      (21, "Bruno Fernandes", "Manchester United"),
      (22, "Paul Pogba", "Manchester United"),
      (23, "Thomas Muller", "Bayern Munich"),
      (24, "Manuel Neuer", "Bayern Munich"),
      (25, "Zlatan Ibrahimovic", "AC Milan"),
      (26, "Gerard Pique", "FC Barcelona"),
      (27, "Luis Suarez", "Atletico Madrid"),
      (28, "Toni Kroos", "Real Madrid"),
      (29, "Joshua Kimmich", "Bayern Munich"),
      (30, "Jordan Henderson", "Liverpool")
    ).toDF("player_id", "player_name", "club_name")

    val clubs = Seq(
      ("FC Barcelona", "Spain"),
      ("Juventus", "Italy"),
      ("Paris Saint-Germain", "France"),
      ("Real Madrid", "Spain"),
      ("Manchester City", "England"),
      ("Bayern Munich", "Germany"),
      ("Liverpool", "England"),
      ("Tottenham Hotspur", "England"),
      ("Atletico Madrid", "Spain"),
      ("AC Milan", "Italy"),
      ("Borussia Dortmund", "Germany"),
      ("Inter Milan", "Italy"),
      ("Manchester United", "England")
    ).toDF("club_name", "country")

    (players, clubs)
  }

  /*
    Broadcast has join is recommended when:
      When one data set is much smaller than the other (and within the
      default config of 10 MB, or more if you have sufficient memory)

    How can we see the size of a dataframe in memory?

    With this method! After you call this, you can check out the WebUI
    and the Storage tab will show you the on heap memory usage!
   */
  def getSizeOfDataframe(spark: SparkSession, df: DataFrame): Unit = {
    df.cache()
    df.count() // Trigger action to cache the DataFrame
  }
}
