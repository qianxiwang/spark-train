package Accumulators.work

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用计数器完成Spark作业的处理数据量,用来实现“全链路监控”
  */
object AccumulatorsApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("AccumulatorsApp")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("F://data/emp.txt")
    val errorAccum = sc.longAccumulator("ErrorAccumulator")

    val trueData = rdd.filter(line => {
      val splits = line.split("\t")
      var benefit = ""
      var empno = ""
      try {
        benefit = splits(6) //记录错误的
        empno = splits(0) //记录总的
        if ("".equals(benefit)) {
          errorAccum.add(1)
        }
      } catch {
        case e: Exception => errorAccum.add(1)
      }
      !"".equals(empno)
    }
    )

    val empcount = trueData.count()

    /**
      * AccumulatorsApp 只能触发一次action,如果经过两次action，累加器会执行两次（要么cache RDD，要么只做一次action）
      */
    //    trueData.foreach(println)
    MySQLDao.SaveAccumulatorToMySQL("AccumulatorsApp", errorAccum.value, empcount)

    sc.stop()
  }
}
