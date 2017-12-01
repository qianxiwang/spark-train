package Shuffle.work.sql

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.sql.{SaveMode, SparkSession}


object demo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("demo1").getOrCreate()

    val rdd = spark.sparkContext.textFile("hdfs://192.168.179.160:8020/emp.txt")
    import spark.implicits._
    val empDF = rdd.map(_.split("\t")).map(x => {
      Emp(x(0), x(1), x(2), x(3), x(4), x(5), x(6),x(7))
    }).toDF()

    empDF.write.mode(SaveMode.Overwrite).format("parquet").partitionBy("deptno").save("hdfs://192.168.179.160:8020/emp_res/res02")

    spark.stop()
  }

  case class Emp(empno: String, ename: String, job: String, mgr: String, hiredate: String, sal: String, comm: String, deptno:String)

}
