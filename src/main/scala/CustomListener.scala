package learningSpark

import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent,
  QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming._

/** We can use this class to setup a custom
 * listener for our SparkStreaming Queries!
 */
class CustomListener extends StreamingQueryListener {
  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    println("Query started: " + event.id)
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    println("Query terminated: " + event.id)
  }

  /**
   * Adjust this progress method based on your needs!
   */
  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    // println("Query made progress: " + event.progress)
    // we can see what are we interested in,
    // let's only see numRowsDroppedByWatermark
    println("Query made progress: numRowsDroppedByWatermark: " +
      event.progress.stateOperators(0).numRowsDroppedByWatermark)
  }
}