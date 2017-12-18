package spark.core

import org.apache.spark.{SparkConf, SparkContext}


object TestHeightApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TestHeightApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val datafile = sc.textFile("F://data/Student.txt")
    val xingbie = datafile.map(x => x.split(" ")(1).contains("M")).count()


    println(xingbie)

    sc.stop()
  }
}
