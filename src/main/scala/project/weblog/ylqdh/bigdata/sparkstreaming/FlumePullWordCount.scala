package project.weblog.ylqdh.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
  Spark Streaming直接采集flume的avro sink过来的数据
  用的是pull模式，所以需要先启动flume，再启动streaming端
 */
object FlumePullWordCount {
  def main(args: Array[String]): Unit = {

    if(args.length != 2) {
      System.err.println("Usage: FlumePullWordCount <hostname> <port>")
      System.exit(1)
    }

    val Array(hostname,port) = args

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("FlumePullWordCount")
    val ssc = new StreamingContext(sparkconf,Seconds(5))

    // TODO... 使用SparkStreaming 整合Flume
    val flumeStream = FlumeUtils.createPollingStream(ssc,hostname,port.toInt)

    flumeStream.map(x => new String(x.event.getBody.array()).trim)   // 获取flume上发送过来的信息
      .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)    // 对接收到的信息进行wordCount
      .print()      // 打印结果


    ssc.start()
    ssc.awaitTermination()
  }

}
