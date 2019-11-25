package project.weblog.ylqdh.bigdata.sparkstreamingTest

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import project.imooc.ylqdh.bigdata.streaming.utils.ParamsConf

object kafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafkaDirectWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    // TODO ... 消费数据
    // messages 是全部的数据
    val messages = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](ParamsConf.topic,ParamsConf.kafkaParams))

    // x.value获得数据里的值
    messages.map(x => x.value())
        .flatMap(_.split(" "))
        .map(x => (x,1))
        .reduceByKey(_+_)
        .print()

    ssc.start()
    ssc.awaitTermination()
  }

}
