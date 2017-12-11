package spark.streaming

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/12/8.
  */
object TransformApp {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")
    val sc = new SparkContext(sparkconf);

    //DB或者是接口中的黑名单
    val bloakTuple = new ListBuffer[(String,Boolean)]
    bloakTuple.append(("huhu",true))
    val blackRDD = sc.parallelize(bloakTuple)


    //外部进来的当前批次的数据
    val input = new ListBuffer[(String,String)]
    input.append(("qianxi","banzhang..."))
    input.append(("duizhang","languifang..."))
    input.append(("huhu","egg..."))
    val inputRDD = sc.parallelize(input)

    inputRDD.leftOuterJoin(blackRDD).filter(line =>{
      line._2._2.getOrElse(false) !=true
    }).map(line => line._2._1).foreach(println)

    sc.stop()
  }
}
