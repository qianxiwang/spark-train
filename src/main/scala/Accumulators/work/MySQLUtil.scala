package Accumulators.work

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * MySQL连接工具类
  */
object MySQLUtil {
  def getConn() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=root&password=000000")
  }

  def release(conn: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      conn.close()
    }
  }
}
