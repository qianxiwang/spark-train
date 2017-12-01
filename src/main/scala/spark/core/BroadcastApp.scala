package spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播变量的使用
  */
object BroadcastApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("BroadcastApp")
    val sc = new SparkContext(sparkConf)

    val peopleInfo = sc.parallelize(Array(("110", "huhuniao"), ("222", "loser"))).map(x => (x._1, x))
    val peopleSchoolInfo = sc.parallelize(Array(("110", "ustc", "211"), ("111", "xxxx", "001"))).map(x => (x._1, x))

    peopleInfo.join(peopleSchoolInfo).map(x => {
      x._1 + "," + x._2._1._2 + "," + x._2._2._2
    }).foreach(println)

    sc.stop()
  }
}
