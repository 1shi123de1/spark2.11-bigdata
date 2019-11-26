package project.weblog.ylqdh.bigdata.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import project.imooc.ylqdh.bigdata.streaming.utils.ParamsConf

/**
  * 日志数据清洗
  * 数据清洗结果如下：
(ip,yyyyMMddHHmmss,courseID,statuCode,referer)
(46.30.10.167,20191125110709,131,200,-)
  */
object LogDataClean {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafkaStreamingApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(60))

    val messages = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](ParamsConf.topic,ParamsConf.kafkaParams))

    // x.value获得数据里的值
    val logs = messages.map(x => x.value())
    val cleanData = logs.map(line => {

      val infos = line.split("\t")
      // 筛选一行数据长度为5的行，其他的舍弃
      if (infos.length != 5) {
        // 如果跳过此条数据
      }
      val courseHtml = infos(2).split(" ")(1)
      var courseID = 0

      // 筛选课程是class的，不要learn、course等其他的
      if (courseHtml.contains("class")) {
        val classCou = courseHtml.split("/")(2)
        courseID = classCou.substring(0,classCou.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0),DateUtil.parseToMinute(infos(1)),courseID,infos(3).toInt,infos(4))

    })

    // 把课程号为0的数据过滤掉
    cleanData.filter(clicklog => clicklog.courseID != 0).print()

    ssc.start()
    ssc.awaitTermination()

  }
}