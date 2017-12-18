package sprak.sql

import org.apache.spark.sql.SparkSession

/**
  *
  */
object a {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("DFjoin").getOrCreate()
    import spark.implicits._

    val textDF = spark.sparkContext.textFile("F://data/a.txt").map(_.split(",")).map(x => Schema(x(0).toInt,x(1).toLong,x(2).toInt,x(3).toLong)).toDF()

//    textDF.show()

    textDF.createOrReplaceTempView("info")

    val a = spark.sql("select id,time,sum(money) from info  where hour between 483402317400 and 1483402318400 group by id").show()


    spark.stop()
  }
  case class Schema(id:Int,time:Long,money:Int,hour:Long);

}
