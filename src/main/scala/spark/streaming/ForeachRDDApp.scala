package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}


/**
  * connection pool的方式实现
  */
object ForeachRDDApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachRDDApp")
    val ssc = new StreamingContext(sparkConf,Seconds(10))

//    ssc.checkpoint("hdfs://192.168.179.160:8020/user/root/checkpoint/")
//    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))


    val lines = ssc.socketTextStream("192.168.179.160", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val result = pairs.reduceByKey(_+_)
//    val result = pairs.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))

    result.foreachRDD( rdd =>{
      rdd.foreachPartition(partitionOfRecords =>{
        val connect = ConnectionPool.getConnection
        connect.setAutoCommit(false)

        partitionOfRecords.foreach(record =>{
          val sql = "insert into wc (word,count) values ('"+record._1+"','"+record._2+"')"
          val stmt = connect.createStatement().execute(sql)
        })
        connect.commit()
      }
      )
    }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 这个batch中key对应的新的值        （hello,1）(hello,1）....
    */
  val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
    val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (word, sum)
    state.update(sum)
    output
  }

}
