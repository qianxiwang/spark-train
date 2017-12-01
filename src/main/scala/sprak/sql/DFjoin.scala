package sprak.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 两个DataFrame进行Join
  */
object DFjoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DFjoin").getOrCreate()

    val props = new Properties()
    props.put("driver","com.mysql.jdbc.Driver")

    val url = "jdbc:mysql://192.168.179.160:3306/metastore?user=root&password=000000"
    val table= "TBLS"
    val TBLSDF =spark.read.format("jdbc").jdbc(url,table,props)

    val url2="jdbc:mysql://192.168.179.160:3306/metastore?user=root&password=000000"
    val table2= "DBS"
    val DBSjDF =spark.sqlContext.read.format("jdbc").jdbc(url2,table2,props)

    TBLSDF.join(DBSjDF,TBLSDF.col("DB_ID")===DBSjDF.col("DB_ID")).select(TBLSDF.col("DB_ID"),TBLSDF.col("TBL_NAME"),DBSjDF.col("DB_ID"),DBSjDF.col("NAME")).show()


    spark.stop()
  }
}
