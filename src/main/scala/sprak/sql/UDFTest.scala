package sprak.sql

import org.apache.spark.sql.SparkSession

object UDFTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("DataFrameApp")
      .getOrCreate()

    runtest(spark)

    spark.stop()
  }

  private def runtest(spark: SparkSession): Unit = {
    import spark.implicits._
    val ip = spark.sparkContext.textFile("F://data/ips.txt")
    val ipDF = ip.map(_.split("\t")).map(x => Ip(x(0), x(1))).toDF()

    val sumStr = spark.udf.register("sumStr", (str: String) => str.split(",").length)

    //    val ipsDF = ipDF.groupBy("name")
    //      .agg(concat_ws(",", collect_set("address")).as("ips"))
    //      .withColumn("ipsLength", sumStr($"ips")).orderBy($"name")
    //    ipsDF.show()


    //concat_ws实现将多行记录合并成一行
    //collect_set是对某列去重，注意：列的类型必须是string类型
    ipDF.createOrReplaceTempView("log")
    val df = spark.sql("select name,concat_ws(',',collect_set(address)) as dist_address, sumStr(concat_ws(',',collect_set(address))) from log group by name").show()


  }

  case class Ip(name: String, address: String)

}
