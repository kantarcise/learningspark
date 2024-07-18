package learningSpark

import  java.sql.Timestamp

case class SimpleSensorWithTimestamp(device_id: Int,
                                     temperature: Int,
                                     eventTime: Timestamp)