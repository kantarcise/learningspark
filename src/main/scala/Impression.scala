package learningSpark

import java.sql.Timestamp

case class Impression(adId: String,
                      impressionTime: Timestamp,
                      userId: String,
                      click: Boolean,
                      deviceType: String)