package learningSpark

import org.apache.spark.sql.{Encoder, SparkSession}
import java.sql.Timestamp
import java.text.SimpleDateFormat

/**
 * Now with typed transformations only!
 *
 * We do not have to type the schema, thanks to case classes!
 */
object FireCallsDataset {

  // For the first transformation
  case class FireCallTransformed(CallDate: Option[String],
                                 StationArea: Option[String],
                                 Zipcode: Option[Int],
                                 CallDateTimestamp: Option[Timestamp],
                                 CallDateTimestampTwo: Option[Timestamp]
                                )
  // for final selection
  case class FireCallTransformedSelected(CallDateTimestamp: Option[Timestamp],
                                         StationArea: Option[String],
                                         Zipcode: Option[Int],
                                         CallDateTimestampTwo: Option[Timestamp])

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Fire Calls Discovery with Datasets!")
      .master("local[*]")
      .getOrCreate()

    // log level - verbose = False
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Define the data path as a val
    val fireCallsPath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/sf-fire-calls.csv"
    }

    // Read the CSV file into a Dataset
    val fireCallsDS = spark.read
      .format("csv")
      .option("header", value = true)
      .schema(implicitly[Encoder[FireCallInstance]].schema)
      .load(fireCallsPath)
      .as[FireCallInstance]

    println("\nThe schema before, for fireCalls DS \n")
    fireCallsDS.printSchema()

    // Convert strings to timestamps
    val transformedDS = fireCallsDS
      .map(transformCallInstance)

    // Filter and order the dataset
    val orderedSelection = transformedDS
      // not null checks
      .filter(call => call.CallDateTimestamp.isDefined && call.CallDateTimestampTwo.isDefined)
      //
      .filter(_.Zipcode.contains(94118))
      .orderBy("CallDateTimestamp")
      .map(value => FireCallTransformedSelected(value.CallDateTimestamp,
        value.StationArea, value.Zipcode, value.CallDateTimestampTwo)
      )

    println("\n Ordered and filtered fireCalls DS, " +
      "with type casted from string to timestamp \n")
    orderedSelection.show(20)

    println("\nThe schema after casting \n")
    orderedSelection.printSchema()

    spark.stop()
  }

  /**
   * Simplify the transformation with a method
   *
   * We cannot use unix_timestamp or to_timestamp
   * because they return Column objects, and casting
   * them directly to Timestamp isn't correct.
   *
   * @param call
   * @return
   */
  def transformCallInstance(call: FireCallInstance): FireCallTransformed = {

    val dateFormat = new SimpleDateFormat("dd/MM/yyyy")

    val callDateTimestamp = call.CallDate.map { date =>
      new Timestamp(dateFormat.parse(date).getTime)
    }
    val callDateTimestampTwo = call.CallDate.map { date =>
      new Timestamp(dateFormat.parse(date).getTime)
    }
    FireCallTransformed(call.CallDate, call.StationArea, call.Zipcode, callDateTimestamp, callDateTimestampTwo)
  }
}