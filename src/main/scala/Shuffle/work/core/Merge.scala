package Shuffle.work.core

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}


object Merge {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Merge")
    val sc = new SparkContext(sparkConf)
    val conf = new Configuration()
    val fileSystem = FileSystem.get(new URI("hdfs://192.168.179.160:8020"), conf)
    makeCoalesce(fileSystem,"hdfs://192.168.179.160:8020/test/data/*/*/*",1)


    sc.stop()
  }

  def makeCoalesce(fileSystem: FileSystem, filePath: String, coalesceSize: Int): Int = {
    var leng_sum  = 0l
    fileSystem.globStatus(new Path(filePath)).map(x => {
      leng_sum += x.getLen
      null
    })
    println("小文件的大小： " + leng_sum + " B")
   val len:Int =  (leng_sum / 1024 / 1024 / coalesceSize).toInt + 1
    println("合并后的块的数量：" + len +"个")
    len
  }


}
