package learningSpark

import org.apache.spark.sql.SparkSession

// What is a Trait?

/*
In Scala, a trait is a fundamental unit of code reuse and modularity.
Traits are similar to interfaces in Java, but they can also contain
method implementations and fields. Traits can be mixed into classes to
augment their functionality or behavior. They provide a way to
compose behaviors and share code across multiple classes.
 */

trait TraitSparkSessionTest {

  protected val sparkSession: SparkSession = SparkSession
    .builder
    .appName("My Test")
    .master("local[2]")
    .getOrCreate()
}