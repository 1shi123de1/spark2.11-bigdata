package com.ylqdh.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSql_RW_JDBC {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSql_RW_JDBC").setMaster("local[*]")
    // 创建sparkSession对象
    val sparkSession:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取jdbc数据方式一
    val jdbcDF1 = sparkSession.read.format("jdbc")
                  .option("url","jdbc:mysql://172.16.1.46:3306/test")
                  .option("dbtable","keya")
                  .option("user","root")
                  .option("password","123456")
                  .load
    jdbcDF1.show()

    // 读取jdbc数据方式二
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user","root")
    connectionProperties.put("password","123456")
    val jdbcDF2 = sparkSession.read.jdbc("jdbc:mysql://172.16.1.46:3306/test","keya",connectionProperties)
    jdbcDF2.show()

    // 将数据写入jdbc方式一
    // 假设jdbcDF1 已经存好数据
    // mode是模式，append是追加，overwrite是覆盖
    jdbcDF1.write.format("jdbc").mode("append")
      .option("url","jdbc:mysql://172.16.1.46:3306/test")
      .option("dbtable","keya")
      .option("user","root")
      .option("password","123456")
      .save()

    // 将数据写入jdbc方式二
    // 假设jdbcDF2已经存好数据
    jdbcDF2.write.jdbc("jdbc:mysql://172.16.1.46:3306/test","keya",connectionProperties)
  }

}
