package sprak.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrameApp {
  def main(args: Array[String]): Unit = {
    //1.拿到sparkSession
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("DataFrameApp")
      .getOrCreate()

    //    runBasicDataFrameExample(spark)
    //    runInferSchemaExample(spark)
    //    runProgrammaticSchemaExample(spark)
    //    runDataFrameExtExample(spark)
    //    runBuildInFunctionExample(spark)
    //    runUDFLengthFunctionExample(spark)
    //    runUDFLengthFunctionExample(spark)
    runUDFhobbyCountFunctionExample(spark)

    spark.stop()
  }

  private def runUDFhobbyCountFunctionExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val info = spark.sparkContext.textFile("F://data/hobby.txt")
    val infoDF = info.map(_.split(":")).map(x => Hobby(x(0), x(1))).toDF()

    spark.udf.register("hobby_num", (str: String) => str.split(",").length)

    infoDF.createOrReplaceTempView("hobby")
    spark.sql("select name,hobby,hobby_num(hobby) as c from hobby").show()
  }

  /**
    * Spark中UDF函数三部曲：
    * 1.定义函数
    * 2.注册函数
    * 3.使用函数
    *
    * @param spark
    */
  private def runUDFLengthFunctionExample(spark: SparkSession): Unit = {
    val peopleDF = spark.read.format("json").load("F://data/people.json")

    //定义函数
    spark.udf.register("strLength", (str: String) => str.length)

    peopleDF.createOrReplaceTempView("people")
    spark.sql("select name,age,strLength(name) from people").show()
  }

  /**
    * Spark SQL 内置函数
    *
    * @param spark
    */
  private def runBuildInFunctionExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val logDF = spark.sparkContext.textFile("F://data/pvuv.log")
      .map(_.split(",")).map(x => Log(x(0), x(1).trim.toInt)).toDF()

    logDF.groupBy("name").sum().show()
    logDF.groupBy("name").agg(sum("times").as("t")).show()
  }

  private def runDataFrameExtExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val empDF = spark.sparkContext.textFile("F://data/Student.txt")
      .map(_.split(" ")).map(x => Student(x(0), x(1), x(2))).toDF()

    empDF.show()
    empDF.show(false)
    empDF.show(10, false)

  }

  /**
    * converte  rdd to dataframe 2
    *
    * @param spark
    */
  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    val rdd = spark.sparkContext.textFile("F://data/people.txt")
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = rdd.map(x => {
      x.split(",")
    }).map(x => Row(x(0), x(1).trim)) // row => People

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)
    peopleDF.show()
  }

  /**
    * converte  rdd to dataframe 1
    *
    * @param spark
    */
  private def runInferSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val rdd = spark.sparkContext.textFile("F://data/people.txt")
    val df = rdd.map(x => {
      x.split(",")
    }).map(x => People(x(0), x(1).trim.toInt)) // row => People
      .toDF()

    df.show()
    df.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    // 取值的第一种方式：index from zero
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()

    // 取值的第二种方式：byname
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name") + "age：" + teenager.getAs[Int]("age")).show()

  }

  /**
    * DataFrame 的基本操作
    *
    * @param spark
    */
  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
//    val df1 = spark.read.json("F://data/people.json")
    val df = spark.read.format("json").load("F://data/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()

    import spark.implicits._

    df.select($"name", $"age" + 1 as ("hhhhh")).show()
    df.filter($"age" > 21).show()
  }

  case class Hobby(name: String, hobby: String)

  case class Log(name: String, times: Int)

  case class Student(id: String, name: String, pho: String)

  case class People(name: String, age: Int)

}
