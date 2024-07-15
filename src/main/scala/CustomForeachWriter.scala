package learningSpark

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.sql.ForeachWriter

// This class is used in the WordCountToPostgres example!
class CustomForeachWriter(url: String,
                          user: String,
                          password: String) extends ForeachWriter[WordCount] {

  var connection: Connection = _
  var statement: PreparedStatement = _

  def open(partitionId: Long, epochId: Long): Boolean = {
    // Open connection
    connection = DriverManager.getConnection(url, user, password)
    val createTableSQL = """
      CREATE TABLE IF NOT EXISTS wordcount (
        value TEXT PRIMARY KEY,
        count BIGINT
      );
    """
    connection.createStatement().execute(createTableSQL)
    statement = connection.prepareStatement(
      "INSERT INTO wordcount (value, count) VALUES (?, ?) ON CONFLICT (value) DO UPDATE SET count = EXCLUDED.count")
    true
  }

  def process(record: WordCount): Unit = {
    // Write each record
    statement.setString(1, record.value)
    statement.setLong(2, record.count)
    statement.executeUpdate()
  }

  def close(errorOrNull: Throwable): Unit = {
    // Close the connection
    if (statement != null) statement.close()
    if (connection != null) connection.close()
  }
}
