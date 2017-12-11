package sprak.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by Administrator on 2017/12/5.
  */
object DataFrameTest2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DataFrameTest2").getOrCreate()

    import spark.implicits._

    // 第一步，构造出元素为Row的普通RDD
    val rdd = spark.sparkContext.textFile("F://data/people.txt").map(_.split(",")).map( line =>{
        Row(line(0),line(1).toInt)
    })

    // 第二步，编程方式动态构造元数据(必须按照字段顺序)
    val schema = StructType(Array(
      StructField("name",StringType),
      StructField("age",IntegerType)
    ))

    // 第三步，进行RDD到DataFrame的转换
    val peopleDF= spark.createDataFrame(rdd,schema)
//    peopleDF.show()


    //取值的时候一定要导入：隐式转换
    // 取值的第一种方式：index from zero
    peopleDF.map(people => "Name: " + people(0) + " " + "age:" + people(1)).show(false)

    // 取值的第二种方式：byname
    peopleDF.map(teenager => "Name: " + teenager.getAs[String]("name") + "  " +"age：" + teenager.getAs[Int]("age")).show(false)

    spark.stop()
  }
}
