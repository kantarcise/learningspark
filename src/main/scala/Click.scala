package learningSpark

import java.sql.Timestamp

case class Click(adId: String,
                 clickTime: Timestamp,
                 country: String)