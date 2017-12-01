package Shuffle.work.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 输出：指定输入的deptno的输出文件数(30写出来2个文件)
  * deptno=10
  * xxxx
  * deptno=20
  * yyyy
  * deptno=30
  * zzzz1
  * zzzz2
  */
object Demo2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Demo2")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("hdfs://192.168.179.160:8020/emp.txt")
    val pairrdd = rdd.map(line => {
      val splits = line.split("\t")
      (splits(7), line)
    })

    val deptno = "30"
    val majorrdd = pairrdd.filter(x => deptno.equals(x._1))
    val otherrdd = pairrdd.filter(x => !deptno.equals(x._1))

    val resrdd = majorrdd.coalesce(2).union(otherrdd.coalesce(1))
//    resrdd.saveAsHadoopFile("hdfs://192.168.179.160:8020/spark_core_res02",
//      classOf[String],
//      classOf[String],
//      classOf[RDDMultipleTextOutputFormat]
//    )

    sc.stop()
  }
}

//class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
//  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
//    (key + "/" + name)
//}
