package sprak.sql

import java.sql.DriverManager


object ThiftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://192.168.179.160:10000", "root", "")
    val stmt = conn.prepareStatement("select empno,ename from emp limit 5")
    val rs = stmt.executeQuery()
    while (rs.next()) {
      println(rs.getInt("empno") + ":" + rs.getString("ename"))
    }

  }
}
