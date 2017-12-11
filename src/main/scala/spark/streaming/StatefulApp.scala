package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * updateStateByKey 方法的使用   (这是原始的办法)
  */
object StatefulApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulApp")
    val ssc = new StreamingContext(sparkConf,Seconds(10))

    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("192.168.179.160", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.updateStateByKey(updateFunc)
    wordCounts.print()


    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 这个batch中key对应的新的值        （hello,1）(hello,1）....
    */
  val updateFunc = (currentValue: Seq[Int], preValue: Option[Int]) => {
    val currentCount = currentValue.sum
    val previousCount = preValue.getOrElse(0)
    Some(currentCount + previousCount)
  }
}
