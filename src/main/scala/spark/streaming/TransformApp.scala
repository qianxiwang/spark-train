package spark.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * 黑名单过滤
  */
object TransformApp {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")
    val sc = new SparkContext(sparkconf);
    val ssc = new StreamingContext(sc,Seconds(10))
    val lines = ssc.socketTextStream("192.168.179.160",9999)


    val bloakTuple = new ListBuffer[(String, Boolean)]
    bloakTuple.append(("huhu", true))
    val blackRDD = sc.parallelize(bloakTuple)

    val clicklog = lines.map(x =>{
      (x.split(",")(0),x)
    }).transform(rdd =>{
      rdd.leftOuterJoin(blackRDD)
    }).filter( x=> x._2._2.getOrElse(false) !=true)
      .map( x=> x._2._1)

    //TODO.....  后续才做正常的业务逻辑

    clicklog.print()

    rdd_black(sc)
    sc.stop()

//    ssc.start()
//    ssc.awaitTermination()

  }

   def rdd_black(sc: SparkContext) = {
    //DB或者是接口中的黑名单
    val bloakTuple = new ListBuffer[(String, Boolean)]
    bloakTuple.append(("huhu", true))
    val blackRDD = sc.parallelize(bloakTuple)


    //外部进来的当前批次的数据
    val input = new ListBuffer[(String, String)]
    input.append(("qianxi", "banzhang..."))
    input.append(("duizhang", "languifang..."))
    input.append(("huhu", "egg..."))
    val inputRDD = sc.parallelize(input)

    inputRDD.leftOuterJoin(blackRDD).filter(line => {
      line._2._2.getOrElse(false) != true
    }).map(line => line._2._1).foreach(println)
  }
}
