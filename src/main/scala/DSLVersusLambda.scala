package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Calendar

object DSLVersusLambda {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DSLVersusLambda")
      .master("local[*]")
      .getOrCreate()

    // verbose = False
    spark.sparkContext.setLogLevel("ERROR")

    // Import implicits for encoders
    import spark.implicits._

    // Define the data path as a val
    val personFilePath: String = {
      val projectDir = System.getProperty("user.dir")
      s"$projectDir/data/mockPerson.json"
    }

    // to Mock some data to work on,
    // you can use https://www.mockaroo.com/

    val personDS = spark
      .read
      .json(personFilePath)
      .as[Person]

    // we will look for everyone over 40
    val earliestYear = Calendar.getInstance.get(Calendar.YEAR) - 40

    // println(earliestYear)
    // 1984

    // Measure running time for lambda and DSL combined
    val startLambdaDSL = System.nanoTime()

    // TODO - compare times ?
    val lambdaAndDSL = personDS
      // Everyone above 40: First lambda
      .filter(x => x.birthDate.split("/")(2).toInt < earliestYear)
      // Everyone earning more than 80K
      .filter($"salary" > 80000)
      // Last name starts with J: Second lambda
      .filter(x => x.lastName.startsWith("J"))
      // First name starts with D
      .filter($"firstName".startsWith("D"))
      .count()

    val endLambdaDSL = System.nanoTime()
    val durationLambdaDSL = (endLambdaDSL - startLambdaDSL) / 1e9d // convert to seconds

    println(s"Count with lambda and DSL: $lambdaAndDSL")
    // 0.679 seconds
    println(f"Duration with lambda and DSL: $durationLambdaDSL%.3f seconds")

    val startDSLOnly = System.nanoTime()

    val DSLOnly = personDS
      // TODO: IN MM/dd/yyyy format this does not work
      //.filter(year($"birthDate") < earliestYear) // Everyone above 40
      .filter(date_format(to_date($"birthDate", "MM/dd/yyyy"), "yyyy").cast("int") < earliestYear) // Everyone above 40
      .filter($"salary" > 80000) // Everyone earning more than 80K
      .filter($"lastName".startsWith("J")) // Last name starts with J
      .filter($"firstName".startsWith("D")) // First name starts with D
      .count()
    val endDSLOnly = System.nanoTime()

    val durationDSLOnly = (endDSLOnly - startDSLOnly) / 1e9d // convert to seconds

    println(s"Count with DSL only: $DSLOnly")
    // 0.325 seconds
    println(f"Duration with DSL only: $durationDSLOnly%.3f seconds")

    // Sleep for 1 minutes
    Thread.sleep(1000 * 60 * 1)
    //  In the meantime, you can check the Web UI

    // Sleep for 20 sec
    // Thread.sleep(1000 * 20 * 1)

    // close after
    spark.close()
  }
}
