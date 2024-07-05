package learningSpark

// The fields in the case class are Option[Datatype] to
// handle potential null values in the CSV file, which is a
// common practice when dealing with real-world data that may
// contain missing or null entries.
case class FireCallInstance(CallNumber: Option[Int],
                             UnitID: Option[String],
                             IncidentNumber: Option[Int],
                             CallType: Option[String],
                             CallDate: Option[String],
                             WatchDate: Option[String],
                             CallFinalDisposition: Option[String],
                             AvailableDtTm: Option[String],
                             Address: Option[String],
                             City: Option[String],
                             ZipCode: Option[Int],
                             Battalion: Option[String],
                             StationArea: Option[String],
                             Box: Option[String],
                             OriginalPriority: Option[String],
                             Priority: Option[String],
                             FinalPriority: Option[Int],
                             ALSUnit: Option[Boolean],
                             CallTypeGroup: Option[String],
                             NumAlarms: Option[Int],
                             UnitType: Option[String],
                             UnitSequenceInCallDispatch: Option[Int],
                             FirePreventionDistrict: Option[String],
                             SupervisorDistrict: Option[String],
                             Neighborhood: Option[String],
                             Location: Option[String],
                             RowID: Option[String],
                             // TODO - if we make Delay: Option[Float] code errors, why?
                             Delay: Option[Double]
                           )