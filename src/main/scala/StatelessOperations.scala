package learningSpark

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

/**
 * Let's discover stateless operations!
 *
 * In stateless transformations, output mode cannot
 * be `complete` - only `append` or `update`
 *
 * Complete mode is not supported because storing the
 * ever-growing result data is usually costly.
 *
 * This is in sharp contrast with stateful transformations.
 */
object StatelessOperations {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .appName("StatelessOperations")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // make a memory stream to test the Stateless Operations
    val memoryStream: MemoryStream[EcommerceCustomer] = new
        MemoryStream[EcommerceCustomer](id = 1, spark.sqlContext)

    // Add sample data to memory stream
    // this adds the data all at once.
    // for sample data -> checkout addDataPeriodicallyToMemoryStream
    // memoryStream.addData(sampleData)

    // Add sample data every 5 seconds - one by one!
    val addDataFuture = addDataPeriodicallyToMemoryStream(memoryStream, 5.seconds)

    // Make a streaming Dataset from the memory stream
    val customerStream: Dataset[EcommerceCustomer] = memoryStream
      .toDS()
      // for dataframe -> dataset we can use
      //.toDF()
      //.as[EcommerceCustomer]

    // Apply projection and selection operations
    val selectedColumns: Dataset[CustomerTimeSpent] = selectColumns(customerStream)

    // stream selected columns
    // Write the output to the console with a 3-second trigger period
    val queryFirst = selectedColumns
      .writeStream
      .queryName("Stream to Console")
      // only two modes possible, append and update
      // if you try complete, the code will error:
      // Complete output mode not supported when there are
      // no streaming aggregations on streaming DataFrames/Datasets
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .start()

    // let's filter and explode the dataset
    // type information as a hint
    val filteredData: Dataset[EcommerceCustomer] = filterData(customerStream)
    val explodedData: Dataset[EcommerceCustomerWithCategory] = explodeData(filteredData)

    // Write the output to the console with a 3-second trigger period
    val querySecond= explodedData
      .writeStream
      .queryName("Stream 2 to Console")
      // only two modes possible, append and update
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .start()

    // let's map and flatmap the dataset
    // type information as a hint
    val mappedData: Dataset[EcommerceCustomer] = mapData(filteredData)
    val flatMappedData : Dataset[EcommerceCustomer] = flatMapData(mappedData)

    // Write the output to the console with a 3-second trigger period
    val queryThird = flatMappedData
      .writeStream
      .queryName("Stream 3 to Console")
      // only two modes possible, append and update
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .start()

    queryFirst.awaitTermination()
    querySecond.awaitTermination()
    queryThird.awaitTermination()

    // Wait for the data adding to finish (it won't, but in a real
    // use case you might want to manage this better)
    Await.result(addDataFuture, Duration.Inf)
  }

  /**
   * Select some columns from a Dataset, this is stateless.
   *
   * Because we are using typed transformations with Datasets,
   * we have a map() instead of select()
   *
   * @param ds: input dataset
   * @return
   */
  def selectColumns(ds: Dataset[EcommerceCustomer]): Dataset[CustomerTimeSpent] = {
    import ds.sparkSession.implicits._

    ds.map { customer =>
      CustomerTimeSpent(
        Email = customer.Email,
        AvgSessionLength = customer.AvgSessionLength,
        TimeonApp = customer.TimeonApp,
        TimeonWebsite = customer.TimeonWebsite
      )
    }
  }

  /** We can demonstrate select() being stateless - Dataframe Version
   *
   * @param df: input Dataframe
   * @return
   */
  def selectColumnsDataframeVersion(df: DataFrame): DataFrame = {
    df.select("Email", "AvgSessionLength", "TimeonApp", "TimeonWebsite")
  }

  /**
   * Filter the streaming dataset, stateless.
   * @param ds: input dataset
   * @return
   */
  def filterData(ds: Dataset[EcommerceCustomer]): Dataset[EcommerceCustomer] = {
   ds.filter(customer => customer.AvgSessionLength.getOrElse(0.0) > 30)
  }

  /**
   * Let's see how explode() is stateless.
   *
   * But in Dataset API, we should use typed transformations only
   * so we do not use explode() we use flatMap!
   *
   * @param ds: input dataset.
   * @return
   */
  def explodeData(ds: Dataset[EcommerceCustomer]): Dataset[EcommerceCustomerWithCategory] = {
    import ds.sparkSession.implicits._
    // this does not seem so useful (business logic wise),
    // think of it just as an example
    val categories = Seq("Low", "Medium", "High")

    ds.flatMap { customer =>
      categories.map { category =>
        EcommerceCustomerWithCategory(
          Email = customer.Email,
          Address = customer.Address,
          Avatar = customer.Avatar,
          AvgSessionLength = customer.AvgSessionLength,
          TimeonApp = customer.TimeonApp,
          TimeonWebsite = customer.TimeonWebsite,
          LengthofMembership = customer.LengthofMembership,
          YearlyAmountSpent = customer.YearlyAmountSpent,
          SpendingCategory = category
        )
      }
    }
  }

  /**
   * Dataframe version is simply, .explode()!
   * @param df: input Dataframe
   * @return
   */
  def explodeDataDataframeVersion(df: DataFrame): DataFrame = {
    df
      .withColumn("SpendingCategory", explode(array(lit("Low"), lit("Medium"), lit("High"))))
  }

  /**
   * map is another method for Dataset's which is stateless!
   * @param ds: input dataset
   * @return
   */
  def mapData(ds: Dataset[EcommerceCustomer]): Dataset[EcommerceCustomer] = {
    import ds.sparkSession.implicits._

    // just multiply YearlyAmountSpent by 20
    ds.map(customer =>
      EcommerceCustomer(
        customer.Email,
        customer.Address,
        customer.Avatar,
        customer.AvgSessionLength,
        customer.TimeonApp,
        customer.TimeonWebsite,
        customer.LengthofMembership,
        customer.YearlyAmountSpent.map(_ * 20)
      )
    )
  }

  /**
   * Dataframe version is simply making a new Column!
   * @param df: input Dataframe
   * @return
   */
  def mapDataDataframeVersion(df: DataFrame): DataFrame = {
    df.withColumn("AdjustedIncome", col("Income") * 1.1)
  }


  /**
   * Let's also demonstrate how flatMap works and
   * how it is stateless.
   * @param ds: input dataset
   * @return
   */
  def flatMapData(ds: Dataset[EcommerceCustomer]): Dataset[EcommerceCustomer] = {
    import ds.sparkSession.implicits._

    // For each EcommerceCustomer object, flatMap generates
    // a sequence (Seq) of two EcommerceCustomer objects
    ds.flatMap(customer => {
      Seq(
        EcommerceCustomer(customer.Email, customer.Address, customer.Avatar,
          customer.AvgSessionLength, customer.TimeonApp,
          customer.TimeonWebsite, customer.LengthofMembership,
          customer.YearlyAmountSpent),
        EcommerceCustomer(customer.Email, customer.Address, customer.Avatar,
          customer.AvgSessionLength, customer.TimeonApp,
          customer.TimeonWebsite, customer.LengthofMembership,
          customer.YearlyAmountSpent.map(_ * 0.3))
      )
    })
  }

  /**
   * Dataframe version of the flatMap method!
   * @param df: input Dataframe
   * @return
   */
  def flatMapDataDataframeVersion(df: DataFrame): DataFrame = {
    // First DataFrame: identical to the input
    val originalDF = df

    // Second DataFrame: YearlyAmountSpent is 30% of the original value
    val modifiedDF = df
      .withColumn("YearlyAmountSpent", col("YearlyAmountSpent") * 0.3)

    // Combine both DataFrames using union
    originalDF.union(modifiedDF)
  }


  /**
   * Instead of adding the sample data into a MemoryStream all at once,
   * add it incrementally.
   *
   * @param memoryStream: Memory Stream to add the data to.
   * @param interval: interval as the data addition period
   * @return
   */
  def addDataPeriodicallyToMemoryStream(memoryStream: MemoryStream[EcommerceCustomer],
                                        interval: FiniteDuration): Future[Unit] = Future {
    // Generate sample data
    val sampleData = Seq(
      EcommerceCustomer(Some("mstephenson@fernandez.com"), Some("835 Frank Tunnel Wrightmouth, MI 82180-9605"), Some("Violet"), Some(34.49726772511229), Some(12.655651149166752), Some(39.57766801952616), Some(4.082620632952961), Some(587.9510539684005)),
      EcommerceCustomer(Some("hduke@hotmail.com"), Some("4547 Archer Common Diazchester, CA 06566-8576"), Some("DarkGreen"), Some(31.926272026360156), Some(11.109460728682564), Some(37.268958868297744), Some(2.66403418213262), Some(392.2049334443264)),
      EcommerceCustomer(Some("pallen@yahoo.com"), Some("24645 Valerie Unions Suite 582 Cobbborough, DC 99414-7564"), Some("Bisque"), Some(33.000914755642675), Some(11.330278057777512), Some(37.11059744212085), Some(4.104543202376424), Some(487.54750486747207)),
      EcommerceCustomer(Some("riverarebecca@gmail.com"), Some("1414 David Throughway Port Jason, OH 22070-1220"), Some("SaddleBrown"), Some(34.30555662975554), Some(13.717513665142508), Some(36.72128267790313), Some(3.1201787827480914), Some(581.8523440352178)),
      EcommerceCustomer(Some("mstephens@davidson-herman.com"), Some("14023 Rodriguez Passage Port Jacobville, PR 37242-1057"), Some("MediumAquaMarine"), Some(33.33067252364639), Some(12.795188551078114), Some(37.53665330059473), Some(4.446308318351435), Some(599.4060920457634)),
      EcommerceCustomer(Some("alvareznancy@lucas.biz"), Some("645 Martha Park Apt. 611"), Some("Purple"), Some(32.10822629832458), Some(11.330220197723989), Some(37.11055345134848), Some(2.6643056352356213), Some(485.9231306421866)),
      EcommerceCustomer(Some("brownjonathan@boone.com"), Some("831 Butler Drive Suite 382"), Some("MediumVioletRed"), Some(33.7317702153489), Some(11.331530262758431), Some(37.11067354099946), Some(4.104404814897259), Some(487.5704283358163)),
      EcommerceCustomer(Some("woodwilliams@lee.com"), Some("42614 Kathy View Apt. 491"), Some("FireBrick"), Some(33.605541854774196), Some(12.794937207064903), Some(37.53665542195368), Some(4.446254969992963), Some(598.2622174490013)),
      EcommerceCustomer(Some("cohenryan@smith.info"), Some("97300 Jessica Place Apt. 280"), Some("PaleGreen"), Some(34.21952861688577), Some(12.79532315721888), Some(37.53664449320562), Some(4.446238723914258), Some(598.1025151471186)),
      EcommerceCustomer(Some("rosssusan@thompson.biz"), Some("7777 Monica Unions Suite 508"), Some("DarkSlateGray"), Some(33.523292418265735), Some(11.109652110213503), Some(37.26889413498055), Some(2.663830867117576), Some(391.6235658262591)),
      EcommerceCustomer(Some("marsharichardson@garcia.com"), Some("511 Kimberly Heights Apt. 870"), Some("BurlyWood"), Some(32.962799119612216), Some(11.109510747372188), Some(37.26891685058043), Some(2.6640027279812995), Some(392.1912758573157))
    )

    sampleData.foreach { record =>
      memoryStream.addData(record)
      Thread.sleep(interval.toMillis)
    }

  }


  /**
   * We can generate even more data from sample data
   * and of course, save it to our filesystem
   *
   * @param jsonFilePath: File path for json.
   * @param spark: The SparkSession that is working on the data.
   */
  def generateAndSaveData(jsonFilePath: String, spark: SparkSession): Unit = {
    import spark.implicits._

    val sampleData = Seq(
      EcommerceCustomer(Some("mstephenson@fernandez.com"), Some("835 Frank Tunnel Wrightmouth, MI 82180-9605"), Some("Violet"), Some(34.49726772511229), Some(12.655651149166752), Some(39.57766801952616), Some(4.082620632952961), Some(587.9510539684005)),
      EcommerceCustomer(Some("hduke@hotmail.com"), Some("4547 Archer Common Diazchester, CA 06566-8576"), Some("DarkGreen"), Some(31.926272026360156), Some(11.109460728682564), Some(37.268958868297744), Some(2.66403418213262), Some(392.2049334443264)),
      EcommerceCustomer(Some("pallen@yahoo.com"), Some("24645 Valerie Unions Suite 582 Cobbborough, DC 99414-7564"), Some("Bisque"), Some(33.000914755642675), Some(11.330278057777512), Some(37.11059744212085), Some(4.104543202376424), Some(487.54750486747207)),
      EcommerceCustomer(Some("riverarebecca@gmail.com"), Some("1414 David Throughway Port Jason, OH 22070-1220"), Some("SaddleBrown"), Some(34.30555662975554), Some(13.717513665142508), Some(36.72128267790313), Some(3.1201787827480914), Some(581.8523440352178)),
      EcommerceCustomer(Some("mstephens@davidson-herman.com"), Some("14023 Rodriguez Passage Port Jacobville, PR 37242-1057"), Some("MediumAquaMarine"), Some(33.33067252364639), Some(12.795188551078114), Some(37.53665330059473), Some(4.446308318351435), Some(599.4060920457634)),
      EcommerceCustomer(Some("alvareznancy@lucas.biz"), Some("645 Martha Park Apt. 611"), Some("Purple"), Some(32.10822629832458), Some(11.330220197723989), Some(37.11055345134848), Some(2.6643056352356213), Some(485.9231306421866)),
      EcommerceCustomer(Some("brownjonathan@boone.com"), Some("831 Butler Drive Suite 382"), Some("MediumVioletRed"), Some(33.7317702153489), Some(11.331530262758431), Some(37.11067354099946), Some(4.104404814897259), Some(487.5704283358163)),
      EcommerceCustomer(Some("woodwilliams@lee.com"), Some("42614 Kathy View Apt. 491"), Some("FireBrick"), Some(33.605541854774196), Some(12.794937207064903), Some(37.53665542195368), Some(4.446254969992963), Some(598.2622174490013)),
      EcommerceCustomer(Some("cohenryan@smith.info"), Some("97300 Jessica Place Apt. 280"), Some("PaleGreen"), Some(34.21952861688577), Some(12.79532315721888), Some(37.53664449320562), Some(4.446238723914258), Some(598.1025151471186)),
      EcommerceCustomer(Some("rosssusan@thompson.biz"), Some("7777 Monica Unions Suite 508"), Some("DarkSlateGray"), Some(33.523292418265735), Some(11.109652110213503), Some(37.26889413498055), Some(2.663830867117576), Some(391.6235658262591)),
      EcommerceCustomer(Some("marsharichardson@garcia.com"), Some("511 Kimberly Heights Apt. 870"), Some("BurlyWood"), Some(32.962799119612216), Some(11.109510747372188), Some(37.26891685058043), Some(2.6640027279812995), Some(392.1912758573157))
    )

    // Generate more data
    val extendedData = (1 to 1000).flatMap(_ => sampleData)

    // Convert to DataFrame and save as JSON
    val df = extendedData.toDF()
    df.write.json(jsonFilePath)
  }
}
