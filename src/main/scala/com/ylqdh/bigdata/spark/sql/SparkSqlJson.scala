package com.ylqdh.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlJson {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSqlJson").setMaster("local[*]")
    // 创建sparkSession对象
    val sparkSession:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 从文件源中创建dataFrame
    val frame = sparkSession.read.json("in/weather.json")

    // 直接展示数据
    frame.show()

    // 用sql方式访问数据
    frame.createOrReplaceTempView("weather")
    sparkSession.sql("select * from weather").show

    sparkSession.stop()
  }
}

