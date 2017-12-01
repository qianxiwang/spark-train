package spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用户访问量的Top5
  */
object PageViewsApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("PageViewsApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val pageviews = sc.textFile("F://data/page_views.dat")
    //1.获取用户id
    val userids = pageviews.map(x => (x.split("\t")(5), 1))
    userids.reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)).take(5).foreach(println)

    sc.stop()
  }
}
