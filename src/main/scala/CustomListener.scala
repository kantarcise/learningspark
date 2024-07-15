package learningSpark

import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming._

// we can use this class to setup a custom
// listener for our SparkStreaming Queries!
class CustomListener extends StreamingQueryListener {
  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    println("Query started: " + event.id)
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    println("Query terminated: " + event.id)
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    println("Query made progress: " + event.progress)
  }
}