package project.weblog.ylqdh.bigdata.sparkstreamingTest

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
  Spark Streaming netstat WordCount测试

  nc -lk 8989
 */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")

    /*
     创建StreamingContext需要两个参数：SparkConf、batch interval
    */
    val ssc = new StreamingContext(sparkconf,Seconds(5))

    val lines = ssc.socketTextStream("172.16.13.152",8989)

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
