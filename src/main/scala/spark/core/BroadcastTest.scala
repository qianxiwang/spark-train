package spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * map-side-join
  * 取出小表中出现的用户与大表关联后取出所需要的信息
  **/
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("BroadcastTest")
    val sc = new SparkContext(sparkConf)

    val peopleInfo = sc.parallelize(Array(("110", "huhuniao"), ("222", "loser"))).collectAsMap()
    val peopleSchoolInfo = sc.parallelize(Array(("110", "ustc", "211"), ("111", "xxxx", "001")))

    //将需要关联的小表进行关联
    val broadcastVar = sc.broadcast(peopleInfo)

    /**
      * 使用mapPartition而不是用map，减少创建broadCastMap.value的空间消耗
      * 同时匹配不到的数据也不需要返回（）
      **/

    //第一种方式：使用for的守卫
    val res1 = peopleSchoolInfo.mapPartitions(iter => {
      val peopleMap = broadcastVar.value
      for {
        (x, y, z) <- iter
        if (peopleMap.contains(x))
      } yield (x, peopleMap.getOrElse(x, ""), y)
    }).foreach(println)

    //第二种方式：使用ArrayBuffer
    val res2 = peopleSchoolInfo.mapPartitions(iter => {
      val peopleMap = broadcastVar.value
      val arrayBuffer = ArrayBuffer[(String, String, String)]()
      iter.foreach { case (x, y, z) => {
        if (peopleMap.contains(x)) {
          arrayBuffer += ((x, peopleMap.getOrElse(x, ""), y))
        }
      }
      }
      arrayBuffer.iterator
    }).foreach(println)

    sc.stop()
  }
}
