package spark.core

import org.apache.spark.{SparkConf, SparkContext}


object SortWordCountApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCountApp")
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(args(0))
    val wc = textFile.flatMap(_.split("\t")).map((_, 1)).reduceByKey(_ + _)

    val sorted = wc.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    wc.saveAsTextFile(args(1))
    sc.stop()
  }
}
