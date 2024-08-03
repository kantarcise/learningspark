package learningSpark

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory
import org.apache.logging.log4j.core.config.Configurator

/**
 * Setting up a proper log4j2.properties is still work in progress
 *
 * But we can setup our loggers programmatically!
 *
 * Check out configureLogging()
 */
object MnmCandiesDatasetWithLogger {

  // Define case class for aggregation result
  case class StateColorCount(State: String, Color: String, Total: Long)

  // Define an aggregator for counting
  object CountAggregator extends Aggregator[Mnm, Long, Long] {
    def zero: Long = 0L
    def reduce(b: Long, a: Mnm): Long = b + 1
    def merge(b1: Long, b2: Long): Long = b1 + b2
    def finish(reduction: Long): Long = reduction
    def bufferEncoder: Encoder[Long] = Encoders.scalaLong
    def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  // just MnmCandiesDatasetWithLogger
  val className = this.getClass.getSimpleName

  def main(args: Array[String]): Unit = {

    // Programmatically configure Log4j2
    // You can make a logs directory to save your logs!
    configureLogging(s"/tmp/$className.log")

    // Initialize the logger
    val logger: Logger = LogManager.getLogger(this.getClass.getName)
    logger.info("This is a Test Log!")

    // define our SparkSession
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()

    // verbose = False
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Define the data path as a var
    var mnmFilePath: String = ""

    if (args.length != 0) {
      mnmFilePath = args(0)
    } else {
      mnmFilePath = {
        val projectDir = System.getProperty("user.dir")
        s"$projectDir/data/mnm_dataset.csv"
      }
    }

    // Read the data into a Dataset
    val mnmDS: Dataset[Mnm] = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFilePath)
      .as[Mnm]

    logger.info("Dataset read!")

    // Aggregate counts of all colors and group by State
    // and Color using the CountAggregator
    val countMnMDS = mnmDS
      .groupByKey(mnm => (mnm.State, mnm.Color))
      .agg(CountAggregator.toColumn.name("Total"))
      .map { case ((state, color), total) => StateColorCount(state, color, total) }
      .orderBy(desc("Total"))

    countMnMDS.show()

    logger.info("Aggregation Complete!")

    // Filter for California MnMs
    val californiaMnMDS = mnmDS
      .filter(_.State == "CA")
      .groupByKey(mnm => (mnm.State, mnm.Color))
      .agg(CountAggregator.toColumn.name("Total"))
      .map { case ((state, color), total) => StateColorCount(state, color, total) }
      .orderBy(desc("Total"))

    californiaMnMDS.show()

    logger.info("Calculated for California!")

    logger.warn("Spark is about to stop!")

    spark.stop()

  }

  def configureLogging(logFileName: String): Unit = {
    val builder = ConfigurationBuilderFactory
      .newConfigurationBuilder()

    // Define appenders
    val consoleAppender = builder
      .newAppender("Console", "CONSOLE")
      .addAttribute("target", "SYSTEM_OUT")
      .add(builder.newLayout("PatternLayout")
        .addAttribute("pattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"))
    builder.add(consoleAppender)

    val fileAppender = builder.newAppender("File", "FILE")
      .addAttribute("fileName", logFileName)
      .add(builder.newLayout("PatternLayout")
        .addAttribute("pattern", "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"))
    builder.add(fileAppender)

    // Define loggers
    // builder.add(builder.newLogger(s"learningSpark.$className", "INFO")
    //   .add(builder.newAppenderRef("Console"))
    //   .add(builder.newAppenderRef("File"))
    //   .addAttribute("additivity", false))

    // Add Spark's loggers
    // builder.add(builder.newLogger("org.apache.spark", "INFO")
    //   .add(builder.newAppenderRef("Console"))
    //   .add(builder.newAppenderRef("File"))
    //   .addAttribute("additivity", false))

    // Or you can configure different settings for console and file
    // Add Spark's loggers with different levels for console and file
    val sparkConsoleLogger = builder
      .newLogger("org.apache.spark", "WARN")
      .add(builder.newAppenderRef("Console"))
      .addAttribute("additivity", false)
    builder.add(sparkConsoleLogger)

    val sparkFileLogger = builder
      .newLogger("org.apache.spark", "INFO")
      .add(builder.newAppenderRef("File"))
      .addAttribute("additivity", false)
    builder.add(sparkFileLogger)

    builder.add(builder.newRootLogger("INFO")
      .add(builder.newAppenderRef("Console"))
      .add(builder.newAppenderRef("File")))

    // Apply configuration
    val config = builder.build()
    Configurator.initialize(config)
  }
}