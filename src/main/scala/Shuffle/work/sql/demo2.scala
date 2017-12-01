package Shuffle.work.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Administrator on 2017/11/29.
  */
object demo2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("demo2").getOrCreate()

    val rdd = spark.sparkContext.textFile("hdfs://192.168.179.160:8020/emp.txt")
    import spark.implicits._
    val empDF = rdd.map(_.split("\t")).map(x => {
      Emp(x(0), x(1), x(2), x(3), x(4), x(5), x(6),x(7))
    }).toDF()

    val dept = "20"
    val majorDF = empDF.select($"deptno" ===dept).show()
    val otherDF = empDF.select(!$"deptno" ===dept).show()
//    val resultDF = majorDF.coalesce(2).union(otherDF.coalesce(1))
//    empDF.write.mode(SaveMode.Overwrite).format("parquet").partitionBy("deptno").save("hdfs://192.168.179.160:8020/emp_res/res01")

    spark.stop()
  }

  case class Emp(empno: String, ename: String, job: String, mgr: String, hiredate: String, sal: String, comm: String, deptno:String)

}
