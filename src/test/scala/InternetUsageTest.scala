package learningSpark

import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

// we can also do our tests with Traits!
class InternetUsageTest extends AnyFunSuite with TraitSparkSessionTest {

  import sparkSession.implicits._

  test("Generate usage data") {
    val dsUsage = InternetUsage.generateUsageData(sparkSession)
    // we know it will make 1001 usage objects
    // they'll all have 3 columns
    assert(dsUsage.count() == 1001)
    assert(dsUsage.columns.length == 3)
    dsUsage.show(5)
  }

  test("Display excessive usage") {
    val dsUsage = InternetUsage.generateUsageData(sparkSession)
    val excessUsageds = dsUsage
      .where($"usage" > 900)
      .orderBy($"usage".desc)

    excessUsageds.show(5, truncate = false)
    // after our filtering, there won't be any non excessive user
    assert(excessUsageds.filter($"usage" <= 900).count() == 0)
  }

  test("Filter excessive usage with lambdas") {
    val dsUsage = InternetUsage.generateUsageData(sparkSession)

    val excessUsageds = dsUsage
      .filter(InternetUsage.filterWithUsage(_))
      .orderBy($"usage".desc)

    excessUsageds.show(5, truncate = false)

    // Assert that all entries in the filtered dataset
    // have usage greater than 900
    // Count entries that do not satisfy the filterWithUsage condition
    // and checking if this count is zero
    assert(excessUsageds.filter(!InternetUsage.filterWithUsage(_)).count() == 0)
  }

  test("Display special treatment costs with lambdas") {
    val dsUsage = InternetUsage.generateUsageData(sparkSession)
    val specialTreatmentDS = dsUsage
      .map(u => if (u.usage > 750) u.usage * .15 else u.usage * .50)

    specialTreatmentDS.show(5, truncate = false)
    assert(specialTreatmentDS.count() == dsUsage.count())
  }

}
