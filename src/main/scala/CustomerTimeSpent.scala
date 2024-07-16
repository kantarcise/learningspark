package learningSpark

case class CustomerTimeSpent(Email: Option[String],
                            AvgSessionLength: Option[Double],
                            TimeonApp: Option[Double],
                            TimeonWebsite: Option[Double]
                           )