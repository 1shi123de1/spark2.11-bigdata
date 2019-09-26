package project.imooc.ylqdh.bigdata.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import project.imooc.ylqdh.bigdata.utils.LogFormat

object LogApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LogApp").master("local[*]").getOrCreate()
    import spark.implicits._

    /*
    自己定义format里面的解析类，我写了解析，但不知道里面的规范
    所以在Java中写方法处理文件，然后在spark类中调用方法
    spark直接读处理好的文件
    val logDF = spark.read.format("").option("path","in/access.log").load()
     */

    // 在Java中处理日志为指定格式
    val logfomat = new LogFormat()
    logfomat.format("in/access.log","in/access_format.log")

    // 读取处理好的日志文件，并把txt格式的文件转换为DataFrame
    val txtRDD = spark.sparkContext.textFile("in/access_format.log")
    val schemaString = "ip,province,city,operator,time,url,method,protocal,status,bytesent,referer,browserName,browserVersion,osName,osVersion"
    val fields = schemaString.split(",").map(fieldName => StructField(fieldName,StringType,nullable = true))
    val schema = StructType(fields)
    val rowRDD = txtRDD.map(_.split("\t"))
                      .map(cells => Row(cells(0),cells(1),cells(2),cells(3),cells(4),cells(5),cells(6),cells(7),cells(8),cells(9),cells(10),cells(11),cells(12),cells(13),cells(14)))
    val logDF = spark.createDataFrame(rowRDD,schema)

    logDF.show()


    spark.stop()


  }
}

