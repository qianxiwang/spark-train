package Shuffle.work.core

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 以emp.txt为例测试
  * 按照deptno进行输出
  * deptno=10
  * xxxx
  * deptno=20
  * yyyy
  * deptno=30
  * zzzz
  */
object Demo1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Demo1")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("hdfs://192.168.179.160:8020/emp.txt")
    val pairrdd = rdd.map(line => {
      val splits = line.split("\t")
      (splits(7), line)
    }).partitionBy(new HashPartitioner(3))
//      .saveAsHadoopFile("hdfs://192.168.179.160:8020/spark_core_res01",
//                classOf[String],
//                classOf[String],
//                classOf[RDDMultipleTextOutputFormat]
//              )

    sc.stop()
  }
}

//class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
//  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
//    (key + "/" + name)
//}
