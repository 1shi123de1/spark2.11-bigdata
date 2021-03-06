package com.ylqdh.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
   程序启动后，启动kafka生产者，生产数据
   kafka-console-producer.sh --broker-list szgwnet01:9092,szgwnet02:9092,szgwnet03:9092 --topic ylqdh
 */

// 窗口函数
object SparkStreamingWindow {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingUpdateStateKafka")

    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf,Seconds(3))

    // 保存数据的状态，需要设置检查点路径
    streamingContext.sparkContext.setCheckpointDir("ckpt")


    // 从kafka中采集数据
    val kafkaDStream = KafkaUtils.createStream(
      streamingContext,
      "szgwnet01:2181,szgwnet02:2181,szgwnet03:2181",
      "ylqdhBigdata",
      Map("ylqdh"->3)
    )

    // 窗口大小应该为采集周期的整数倍，窗口滑动的步长也应该为周期的整数倍
    val winddowDStream = kafkaDStream.window(Seconds(9),Seconds(3))

    // 将采集的数据进行分解(扁平化)
    val wordDStream = winddowDStream.flatMap(t=>t._2.split(" "))

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
