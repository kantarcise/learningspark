package learningSpark

import learningSpark.InternetUsage.computeCostUsage
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

// we can also do our tests with Traits!
class InternetUsageTest extends AnyFunSuite with TraitSparkSessionTest {

  // This Spark Session is taken from the Trait -> TraitSparkSessionTest
  import sparkSession.implicits._

  test("Generate usage data") {
    val dsUsage = InternetUsage.generateUsageData(sparkSession)
    // we know generateUsageData will make 1001 Usage objects
    // They'll all have 3 columns
    assert(dsUsage.count() == 1001)
    assert(dsUsage.columns.length == 3)
    dsUsage.show(5)
  }

  test("Display excessive usage") {
    val dsUsage = InternetUsage.generateUsageData(sparkSession)

    // We can filter based on a Column
    val excessUsageDS = dsUsage
      .where($"usage" > 900)
      .orderBy($"usage".desc)

    excessUsageDS.show(5, truncate = false)
    // after our filtering, there won't be any non excessive user
    assert(excessUsageDS.filter($"usage" <= 900).count() == 0)
  }

  test("Filter excessive usage with lambdas") {
    val dsUsage = InternetUsage.generateUsageData(sparkSession)

    val excessUsageDS = dsUsage
      // We can use the method filterWithUsage
      .filter(value => InternetUsage.filterWithUsage(value))
      .orderBy($"usage".desc)

    excessUsageDS.show(5, truncate = false)

    // Assert that all entries in the filtered dataset
    // have usage greater than 900
    // Count entries that do not satisfy the filterWithUsage condition
    // and checking if this count is zero
    assert(excessUsageDS.filter(!InternetUsage.filterWithUsage(_)).count() == 0)
  }

  test("Display special treatment costs with lambdas") {
    val dsUsage = InternetUsage.generateUsageData(sparkSession)
    val specialTreatmentDS = dsUsage
      .map(u => if (u.usage > 750) u.usage * .15
      else u.usage * .50)

    specialTreatmentDS.show(5, truncate = false)
    assert(specialTreatmentDS.count() == dsUsage.count())
  }

  test("Special Treatment Cost with ") {
    val dsUsage = InternetUsage.generateUsageData(sparkSession)

    val specialPrices: Dataset[SpecialPrices] = dsUsage
      // we can just import the method and use it directly
      // computeCostUsage instead of InternetUsage.computeCostUsage
      .map((u: Usage) => SpecialPrices(computeCostUsage(u.usage)))
      // .show(5, truncate = false)

    // special prices should have 1001 instances
    // all with NewPrice column.
    assert(specialPrices.count() == 1001)
    assert(specialPrices.isInstanceOf[Dataset[SpecialPrices]])
  }
}
