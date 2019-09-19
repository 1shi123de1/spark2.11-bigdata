package com.ylqdh.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object SparkBroadCast {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("SparkBroadCast")
    val sc = new SparkContext(config)

    val list = List((1,2),(1,3),(1,4),(1,5))
    // 使用广播变量减少数据的传输
    // 构建广播变量
    val broadCast = sc.broadcast(list)

    //使用广播变量
    println(broadCast.value)
  }
}
