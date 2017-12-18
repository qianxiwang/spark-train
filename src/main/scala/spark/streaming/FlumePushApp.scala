package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Flume + Streaming   (Push)
  */
object FlumePushApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePushApp")
    val ssc = new StreamingContext(sparkConf,Seconds(10))

    val lines = FlumeUtils.createStream(ssc, "192.168.179.160", 44443)

    val words = lines.map(x=> new String(x.event.getBody.array()).trim).flatMap(_.split(" "))
    val pairs = words.map(word => (word,1))
    val wordcount = pairs.reduceByKey(_+_)
    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
