package Shuffle.work.core

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil


/**
  *
  */
object ScalaTest {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.set("fs.default.name","hdfs://192.168.179.160:8020")
    val fileSyatem = FileSystem.get(URI.create("hdfs://192.168.179.160:8020/spark/emp/temp/201711112025/d=20171111/h=20/"),conf)
    val outputPath = "hdfs://192.168.179.160:8020/spark/emp/"
    val loadtime = "201711112025"
    val partition = "/d=20171111/h=20"

    changeFileName(fileSyatem,outputPath,loadtime,partition)
  }

  def changeFileName(fileSystem: FileSystem, outputPath:String, loadtime:String, partition:String): Unit ={
    //得到原本的temp目录
    val paths = SparkHadoopUtil.get.globPath(new Path(outputPath + "temp/" + loadtime + partition + "/*.txt"))
    var times = 0

    paths.map(x=>{
      //将temp目录替换成data目录
      var newlocation = x.toString.replace(outputPath + "temp/" + loadtime,outputPath + "data")
      println("old ：" + newlocation)
      //将.txt文件改名
      newlocation = newlocation.replace("part-r","")
      val index = newlocation.lastIndexOf("/")
      times+=1
      //得到最后结果的目录
      newlocation = newlocation.substring(0, index+1) + loadtime.substring(2,8) + "25" + "-" + times + ".txt"
      println("new ：" + newlocation)
      val resPath = new Path(newlocation)
      if(!fileSystem.exists(resPath.getParent)){
          fileSystem.mkdirs(resPath.getParent)
      }
      fileSystem.rename(x,resPath)
    })
  }

}
