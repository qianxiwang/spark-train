package Shuffle.work.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 你们自己造点数据，原则：deptno给我弄成数据倾斜
  * 取样出来知道哪个deptno是数据倾斜的，结合b)完成
  * deptno=10
  * xxxx
  * deptno=20
  * yyyy
  * deptno=30
  * zzzz1
  * zzzz2
  */
object Demo3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Demo3")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("hdfs://192.168.179.160:8020/emp.txt")
    val pairrdd = rdd.map(line => {
      val splits = line.split("\t")
      (splits(7), line)
    })

    val samplerdd = pairrdd.sample(false, 0.1)
    val count_sample = samplerdd.countByKey().foreach(println)
    //假设30的多

    val deptno = "10"

    val majorrdd = pairrdd.filter(x => deptno.equals(x._1))
    val otherrdd = pairrdd.filter(x => !deptno.equals(x._1))

    val resrdd = majorrdd.coalesce(2).union(otherrdd.coalesce(1))
//    resrdd.saveAsHadoopFile("hdfs://192.168.179.160:8020/spark_core_res03",
//          classOf[String],
//          classOf[String],
//          classOf[RDDMultipleTextOutputFormat]
//        )

    sc.stop()
  }
}

//class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
//  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
//    (key + "/" + name)
//}