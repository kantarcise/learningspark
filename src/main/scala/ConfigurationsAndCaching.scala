package learningSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.storage.StorageLevel


// too see all options about configuration
// https://spark.apache.org/docs/3.5.0/configuration.html
object ConfigurationsAndCaching {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("ConfigurationsAndCaching")
      .master("local[8]")
      // the following config cannot be set after we make the session
      // so we give them in initialization
      // By default spark.dynamicAllocation.enabled is set to false
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.dynamicAllocation.minExecutors", "2")
      .config("spark.dynamicAllocation.schedulerBacklogTimeout", "1m")
      // adjust to your machine!
      .config("spark.dynamicAllocation.maxExecutors", "12")
      .config("spark.dynamicAllocation.executorIdleTimeout", "2min")
      // here are some configurations about Spark executors’ memory
      // and the shuffle service
      // Set executor memory
      .config("spark.executor.memory", "800m")
      // Enable shuffle service
      .config("spark.shuffle.service.enabled", "true")
      // Optionally set shuffle service port
      .config("spark.shuffle.service.port", "7337")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("You can print a single configuration \n")

    printConfig(spark, "spark.sql.join.preferSortMergeJoin")
    printConfig(spark, "spark.driver.host")

    println("Or you can print them all \n")

    // Lets see all configurations
    printConfigs(spark)

    spark.conf.set("spark.sql.shuffle.partitions",
      spark.sparkContext.defaultParallelism)

    println("\n****** Setting Shuffle Partitions to Default Parallelism ****** \n")

    // you will see a new line printed
    printConfigs(spark)

    // lets see spark.sql configs
    seeSparkSQLConfigs(spark)

    // lets change one more!
    getSetProperties(spark)

    // see dynamic Allocations Configs
    sparkDynamicAllocation(spark)

    // see the effect
    printConfigs(spark)

    effectOfCache(spark)

    effectOfPersist(spark)

    cacheATable(spark)

    setPartition(spark)

    // Sleep to check the Web UI
    // go to http://localhost:4040/storage/
    // to see In-memory table dfTable
    Thread.sleep(1000 * 10 * 1)

  }

  /*
    Print all configurations
    While printing to the console, you can change the color easily!
   */
  def printConfigs(session: SparkSession) = {
    // Get conf
    val conf = session.conf.getAll
    println()

    // Print them
    for (k <- conf.keySet) {
      // choose different color for key and values
      println(Console.BLUE + s"${k} -> " + Console.RESET + s"${conf(k)}")
      // println(Console.BLUE + s"${k} -> ${conf(k)}\n") }
    }
  }

  /*
    Print a single configuration
   */
  def printConfig(session: SparkSession, key:String) = {
    // get conf
    val v = session.conf.getOption(key)
    println(s"${key} -> ${v}\n")
  }

  /*
  There are a lot of ways to set and get spark properties.
  There is a precedence between them.
  spark.conf file < cli < SparkApplication

   All these settings will be merged, with any duplicate properties
   reset in the Spark application taking precedence.

   Likewise, values supplied on the command line will supersede
   settings in the configuration file, provided they are not
   overwritten in the application itself.
   */
  def getSetProperties(spark: SparkSession): Unit  = {
    // To set or modify an existing configuration
    // programmatically, first check if the property is modifiable.

    println("is autoBroadcastJoinThreshold modifiable ? " +
      spark.conf.isModifiable("spark.sql.autoBroadcastJoinThreshold"))
    // this is true!
    // Let's try to change it!
    // (more on https://spark.apache.org/docs/latest/sql-performance-tuning.html)

    // learn the default value
    println("default autoBroadcastJoinThreshold value ? " +
      spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
    // 10485760b

    // Let's make it 20 MBs
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",
      20 * 1024 * 1024)

    // see the change
    println("default autoBroadcastJoinThreshold value ? " +
      spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
  }

  def seeSparkSQLConfigs(spark: SparkSession): Unit = {
    println("\nLet's see all spark.sql configurations:\n")
    // Only the Spark SQL–specific Spark configs
    spark.sql("SET -v")
      .select("key", "value")
      // there is 193 of them :O
      .show(200, truncate = false)
  }

  /*
  When you specify compute resources as command-line arguments to
  spark-submit, as we did earlier, you cap the limit.

  This means that if more resources are needed later as tasks queue
  up in the driver due to a larger than anticipated workload,
  Spark cannot accommodate or allocate extra resources.

  If instead you use Spark’s dynamic resource allocation
  configuration, the Spark driver can request more or fewer compute
  resources as the demand of large workloads flows and ebbs.

  In scenarios where your workloads are dynamic—that is, they vary in
  their demand for compute capacity—using dynamic allocation helps to
  accommodate sudden peaks.

  One use case where this can be helpful is streaming, where the data
  flow volume may be uneven.

  Another is on-demand data analytics, where you might have a high
  volume of SQL queries during peak hours.

  Enabling dynamic resource allocation allows Spark to achieve better
  utilization of resources, freeing executors when not in use and
  acquiring new ones when needed
   */
  def sparkDynamicAllocation(spark: SparkSession): Unit = {

    println("Let's see the dynamicAllocation configurations: \n")

    println("Is dynamicAllocation enabled ? " +
      spark.conf.get("spark.dynamicAllocation.enabled"))

    println("What is minExecutors set to ? " +
      spark.conf.get("spark.dynamicAllocation.minExecutors"))

    println("What is schedulerBacklogTimeout set to ? " +
      spark.conf.get("spark.dynamicAllocation.schedulerBacklogTimeout"))

    println("What is maxExecutors set to ? " +
      spark.conf.get("spark.dynamicAllocation.maxExecutors"))

    println("What is executorIdleTimeout ? " +
      spark.conf.get("spark.dynamicAllocation.executorIdleTimeout"))
  }

  /*
  Lets discover about the effects for caching Dataframes.

  Common use cases for caching are scenarios where you will want to access a large
  data set repeatedly for queries or transformations.

  Some examples include:
     • DataFrames commonly used during iterative machine learning training
     • DataFrames accessed commonly for doing frequent transformations
          during ETL or building data pipelines

   Not all use cases dictate the need to cache.

   Some scenarios that may not warrant caching your DataFrames include:
     • DataFrames that are too big to fit in memory
     • An inexpensive transformation on a DataFrame not requiring frequent
     use, regardless of size
   */
  def effectOfCache(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark.range(1 * 10000000)
      .toDF("id").withColumn("square", $"id" * $"id")
    df.cache() // Cache the data

    val (res, tm) = timer(df.count())
    println(s"\nBefore Caching: Count=${res} and time=${tm} milliseconds")

    val (res2, tm2) = timer(df.count())
    println(s"\nAFTER Caching: Count=${res2} and time=${tm2} milliseconds")

    // the manual way - you can delete it if you want

    // val startTime = System.nanoTime()
    // println(s"Count of DF, {df.count()}") // Materialize the cache
    // val endTime = System.nanoTime()
    // val timeDifference = (endTime - startTime)  / 1e9d
    // println(f"Duration Before Caching : $timeDifference%.3f seconds\n")
    //  1.632 seconds

    // val startTimeSecond = System.nanoTime()
    // println(s"Count of DF, {df.count()}") // Now counting from cache
    // val endTimeSecond = System.nanoTime()
    // val timeDifferenceSecond = (endTimeSecond - startTimeSecond)  / 1e9d
    // println(f"Duration AFTER Caching : $timeDifferenceSecond%.3f seconds\n")
    // 0.127 seconds

  }

  /*
  Lets discover about the effects for persisting Dataframes.
 */
  def effectOfPersist(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark.range(1 * 10000000)
      .toDF("id").withColumn("square", $"id" * $"id")

    // Persist the data
    df.persist((StorageLevel.DISK_ONLY))

    val (res, tm) = timer(df.count())
    println(s"\nBefore Persisting: Count=${res} and time=${tm} milliseconds")

    val (res2, tm2) = timer(df.count())
    println(s"\nAFTER Persisting: Count=${res2} and time=${tm2} milliseconds")


    // the manual way - you can delete it if you want

    // val startTime = System.nanoTime()
    // println(s"Count of DF, {df.count()}") // Materialize the cache
    // val endTime = System.nanoTime()
    // val timeDifference = (endTime - startTime)  / 1e9d
    // println(f"Duration before Persisting : $timeDifference%.3f seconds\n")
    // 0.101 seconds

    // this is actually slower ?
    // val startTimeSecond = System.nanoTime()
    // println(s"Count of DF, {df.count()}") // Now reading from disk?
    // val endTimeSecond = System.nanoTime()
    // val timeDifferenceSecond = (endTimeSecond - startTimeSecond)  / 1e9d
    // println(f"Duration AFTER Persisting : $timeDifferenceSecond%.3f seconds\n")
    // 0.111 seconds

    // unpersist after
    df.unpersist()

  }

  def cacheATable(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark
      .range(1 * 10000000).toDF("id")
      .withColumn("square", $"id" * $"id")

    df.createOrReplaceTempView("dfTable")
    spark.sql("CACHE TABLE dfTable")
    println("\n We can also cache tables! Here is the result of count:\n")
    spark.sql("SELECT count(*) FROM dfTable")
      .show()
  }

  def setPartition(spark: SparkSession): Unit = {
    val numDF = spark
      .range(100L * 100 * 100)
      .repartition(12)

    println(s" *** Num of partitions in Dataframe ${numDF.rdd.getNumPartitions}")

    spark.conf.set("spark.sql.shuffle.partitions",
      spark.sparkContext.defaultParallelism)

    println("\nSetting Shuffle partitions to Default parallelism")

    // let's see the effect
    printConfigs(spark)
  }

  /*
    Instead of repeatedly write the timing code, we can use this
    method to time some part of code!

    Taken from the book.
   */
  def timer[A](blockOfCode: => A): (A, Double) = {
    val startTime = System.nanoTime
    val result = blockOfCode
    val endTime = System.nanoTime
    val delta = endTime - startTime
    (result, delta/1000000d)
  }

}
