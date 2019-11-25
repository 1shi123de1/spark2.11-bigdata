package project.weblog.ylqdh.bigdata.sparkstreamingTest

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkaStreamingApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafkaStreamingApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(60))

    val topic = List("ylqdh")
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "szgwnet01:9092,szgwnet02:9092,szgwnet03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "ylqdh.group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    // TODO ... 消费数据
    // messages 是全部的数据
    val messages = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topic,kafkaParams))

    // x.value获得数据里的值
    messages.map(x => x.value()).count()
//        .flatMap(_.split(" "))
      //        .map(x => (x,1))
      //        .reduceByKey(_+_)
        .print()

    ssc.start()
    ssc.awaitTermination()
  }

}
