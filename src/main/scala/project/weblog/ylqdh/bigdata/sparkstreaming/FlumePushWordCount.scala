package project.weblog.ylqdh.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
  Spark Streaming直接采集flume的avro sink过来的数据
  用的是push模式，所以需要先启动streaming端，再启动flume
 */
object FlumePushWordCount {
  def main(args: Array[String]): Unit = {

    if(args.length != 2) {
      System.err.println("Usage: FlumePushWordCount <hostname> <port>")
      System.exit(1)
    }

    val Array(hostname,port) = args

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("FlumePushWordCount")
    val ssc = new StreamingContext(sparkconf,Seconds(5))

    // TODO... 使用SparkStreaming 整合Flume
    // 换成 createPollingStream就是pull模式的
    val flumeStream = FlumeUtils.createStream(ssc,hostname,port.toInt)

    flumeStream.map(x => new String(x.event.getBody.array()).trim)   // 获取flume上发送过来的信息
      .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)    // 对接收到的信息进行wordCount
      .print()      // 打印结果


    ssc.start()
    ssc.awaitTermination()
  }

}
