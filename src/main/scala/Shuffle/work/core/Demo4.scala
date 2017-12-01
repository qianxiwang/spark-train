package Shuffle.work.core

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 按照deptno和hiredate进行分区
  * data/deptno=10/hiredate=yyyy
  * 支持不同文件（新文件或者重跑）
  */
object Demo4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Demo4")
    val sc = new SparkContext(sparkConf)

    val conf = new Configuration()
    val fileSystem = FileSystem.get(new URI("hdfs://192.168.179.160:8020"), conf) //得到hdfs的配置

    val rdd = sc.textFile("hdfs://192.168.179.160:8020/emp.txt") //加载数据
    val pairRDD = rdd.map(line => {
      val splits = line.split("\t")
      ((splits(7) + "/" + splits(4).substring(0,4)), line)
    })

    //定义tmp目录和data目录
    val tmppath = "hdfs://192.168.179.160:8020/test/tmp/"
    val path = "hdfs://192.168.179.160:8020/test/data/"

    //判断tmp目录是否存在，存在就删除
    if (fileSystem.exists(new Path(tmppath))) {
      fileSystem.delete(new Path(tmppath), true)
    }

    //先将数据按照两层目录保存到tmppath中
    pairRDD.saveAsHadoopFile(
      tmppath,
      classOf[String],
      classOf[String],
      classOf[RDDMultipleTextOutputFormat]
    )

    val map = pairRDD.collectAsMap()
    val set = new mutable.HashSet[String]
    map.map(_._1).foreach((x: String) => set.add(x)) //把deptno放到可变集合中

    replaceTempFile(fileSystem, tmppath, path, set)

    sc.stop()
  }

  def replaceTempFile(fileSystem: FileSystem, tmppath: String, path: String, partitions: mutable.HashSet[String]) {
    partitions.foreach(partition => {
      //先删除data目录下的数据
      SparkHadoopUtil.get.globPath(new Path(path + partition + "/part-*")).map(fileSystem.delete(_, false))
      //得到tmp目录下的数据
      val paths = SparkHadoopUtil.get.globPath(new Path(tmppath + partition + "/part-*"))
      paths.map(x => {
        val officialPath = new Path(path + partition)
        if (!fileSystem.exists(officialPath)) {
          fileSystem.mkdirs(officialPath)
        }
        fileSystem.rename(x, officialPath)
      })
    })
  }

}

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  //得到输出的两层目录
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    (key.toString.split("/")(0) + "/" + key.toString.split("/")(1) + "/" + name)
  }
}


