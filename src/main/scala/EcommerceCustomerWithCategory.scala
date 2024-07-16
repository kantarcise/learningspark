package learningSpark

case class EcommerceCustomerWithCategory(Email: Option[String],
                                         Address: Option[String],
                                         Avatar: Option[String],
                                         AvgSessionLength: Option[Double],
                                         TimeonApp: Option[Double],
                                         TimeonWebsite: Option[Double],
                                         LengthofMembership: Option[Double],
                                         YearlyAmountSpent: Option[Double],
                                         SpendingCategory: String
                                        )