package com.ylqdh.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStreamingWordCount")

    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf,Seconds(3))

    // 从指定的端口中采集数据,端口号不能超过9999
    val socketLineDStream = streamingContext.socketTextStream("gwnet01",7777)

    // 将采集的数据进行分解(扁平化)
    val wordDStream = socketLineDStream.flatMap(lines=>lines.split(" "))

    // 将数据转换结构后方便统计分析
    val mapDStream = wordDStream.map((_,1))

    // 将数据进行聚合处理
    val wordSumDStream = mapDStream.reduceByKey(_+_)

    wordSumDStream.print()

    // 不能停止采集程序
    // streamingContext.stop()

    // 启动采集器
    streamingContext.start()
    // Driver等待采集器的执行
    streamingContext.awaitTermination()
  }
}
