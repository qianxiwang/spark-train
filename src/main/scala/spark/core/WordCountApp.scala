package spark.core

import org.apache.spark.{SparkConf, SparkContext}


object WordCountApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCountApp")
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(args(0))
    val wc = textFile.flatMap(_.split("\t")).map((_, 1)).reduceByKey(_ + _)

    //    wc.collect().foreach(println)
    wc.saveAsTextFile(args(1))
    sc.stop()
  }
}
