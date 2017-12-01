package spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算平均年龄（年龄的总和/个数）
  */
object AvgAgeApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AvgAgeApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val dataFile = sc.textFile("F://data/age_data.txt")
    //取出年龄
    val age = dataFile.map(x => x.split(" ")(1))
    //求人数
    val count = dataFile.count()
    //年龄相加/人数
    val age_sum = age.map(age => age.toInt).reduce(_ + _)

    val avgage = age_sum / count
    println(avgage)

    sc.stop()
  }
}
