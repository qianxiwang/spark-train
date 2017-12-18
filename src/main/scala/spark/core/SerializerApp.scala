package spark.core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by Administrator on 2017/12/18.
  */
object SerializerApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    sparkConf.registerKryoClasses(Array(classOf[Info]))

    val sc = new SparkContext(sparkConf)

    val array = new ArrayBuffer[Info]()
    val nameArray = Array[String]("huhuhu","xiaoduizhang","feijige")
    val genderArray = Array[String]("male","female")
    val addressArray = Array[String]("beijing","shanghai","hangzhou","chengdu")

    for(i <-1 to 1000000){
      val name = nameArray(Random.nextInt(3))
      val gender = genderArray(Random.nextInt(2))
      val address = addressArray(Random.nextInt(4))
      val age = Random.nextInt(100)

      array += Info(name,age,gender,address)
    }

    val rdd = sc.parallelize(array)
    rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    rdd.unpersist(true)

    sc.stop()
  }

  case class Info(name:String, age:Int, gender:String, address:String)
}
