package project.weblog.ylqdh.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateFulWordCount {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("StateFulWordCount").setMaster("local[*]")

    /*
     创建StreamingContext需要两个参数：SparkConf、batch interval
    */
    val ssc = new StreamingContext(sparkconf,Seconds(5))

    // 如果使用了stateful的算子，必须要设置checkpoint
    // 在生产环境中，建议把checkpint 目录放在HDFS上
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("172.16.13.152",8989)
    val result = lines.flatMap(_.split(" ")).map((_,1))

    val state = result.updateStateByKey(updateFunc _)

    state.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunc(currentValues:Seq[Int],preValues:Option[Int]):Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }

}
