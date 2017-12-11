package sprak.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/12/5.
  */
object DataFrameTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DataFrameTest").getOrCreate()

    import spark.implicits._

//    val df = spark.read.format("json").load("F://data/people.json")
    val df = spark.read.format("json").option("path","F://data/people.json").load()
//    df.printSchema()
//    df.show(1)
    df.select($"name" ==="Andy").show()
//    df.select("name").show()
    df.select($"name",$"age"+1 as("hhh")).show()
//    df.filter($"age">20).show()
//    df.groupBy("age").count().show()
//    df.createOrReplaceTempView("people")
//    val sqlDF = spark.sql("select * from people")
//    sqlDF.show()

    spark.stop()
  }
}
