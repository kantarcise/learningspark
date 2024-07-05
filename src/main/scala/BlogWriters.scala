package learningSpark

case class BlogWriters(Id: Int,
                       First: String,
                       Last: String,
                       Url: String,
                       Published: String,
                       Hits: Int,
                       Campaigns: Seq[String]
                      )