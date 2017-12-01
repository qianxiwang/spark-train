package sprak.sql

import org.apache.spark.sql.SparkSession

object FtpApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("FtpApp")
      .getOrCreate()

    //第一种方式：
    //    val dataSource = "ftp://192.168.179.160/opt/data/people.json"
    //    spark.sparkContext.addFile(dataSource)
    //    val fileName = SparkFiles.get(dataSource)
    //    val df = spark.read.
    //      format("com.springml.spark.sftp").
    ////      option("host", "192.168.179.160").
    //      option("username", "ftpuser").
    //      option("password", "ftpuser").
    //      option("fileType", "json").
    //      load(fileName).show()

    //第二种方式：
    val df = spark.read.
      format("com.springml.spark.sftp").
      option("host", "192.168.179.160").
      option("username", "ftpuser").
      option("password", "ftpuser").
      option("fileType", "json").
      load("ftp://192.168.179.160/home/ftpuser/people.json").show()

    spark.stop()
  }
}
