package spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/11/20.
  */
object SparkContextApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkContextApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //TODO...  业务逻辑

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data).foreach(println)

    sc.stop()
  }
}
