package learningSpark

import java.sql.Timestamp

case class FireCallInstanceWithTimestamps(CallNumber: Option[Int],
                                          UnitID: Option[String],
                                          IncidentNumber: Option[Int],
                                          CallType: Option[String],
                                          // CallDate: Option[String],
                                          // WatchDate: Option[String],
                                          CallFinalDisposition: Option[String],
                                          // AvailableDtTm: Option[String],
                                          Address: Option[String],
                                          City: Option[String],
                                          Zipcode: Option[Int],
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
                                          ResponseDelayedinMins: Option[Double],
                                          IncidentDate: Option[Timestamp],
                                          OnWatchDate: Option[Timestamp],
                                          AvailableDtTS: Option[Timestamp]
                                         )