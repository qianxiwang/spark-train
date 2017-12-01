package Accumulators.work

import java.sql.{Connection, PreparedStatement}

/**
  * 将处理的数据条数写入mysql
  */
object MySQLDao {
  def SaveAccumulatorToMySQL(appName: String, times: Long, counts: Long): Unit = {
    //    val table = "accumulator"
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    try {
      conn = MySQLUtil.getConn()
      val sql = "INSERT INTO accumulator (appName, errorcount, empnocount) VALUES (?, ?, ?)"
      pstmt = conn.prepareStatement(sql)
      pstmt.setString(1, appName)
      pstmt.setLong(2, times)
      pstmt.setLong(3, counts)
      pstmt.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtil.release(conn, pstmt)
    }
  }
}
