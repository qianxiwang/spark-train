package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Kafka  + Streaming (Receiver方式)
  */
object KafkaReceiverApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverApp")
    val ssc = new StreamingContext(sparkConf,Seconds(10))

    val  Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc,zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word,1))
    val wordcount = pairs.reduceByKey(_+_)
    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
