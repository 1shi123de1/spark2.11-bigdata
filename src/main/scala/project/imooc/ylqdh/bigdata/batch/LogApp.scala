package project.imooc.ylqdh.bigdata.batch

import org.apache.spark.sql.SparkSession
import project.imooc.ylqdh.bigdata.utils.LogFormat

object LogApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LogApp").master("local[*]").getOrCreate()


    /*
    自己定义format里面的解析类，我写了解析，但不知道里面的规范
    所以在Java中写方法处理文件，然后在spark类中调用方法
    spark直接读处理好的文件
    val logDF = spark.read.format("").option("path","in/access.log").load()
     */

    LogFormat logfomat = new LogFormat();



  }
}
