package project.weblog.ylqdh.bigdata.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import project.imooc.ylqdh.bigdata.streaming.utils.ParamsConf

import scala.collection.mutable.ListBuffer

/**
  * 日志数据清洗
  * 数据清洗结果如下：
(ip,yyyyMMddHHmmss,courseID,statuCode,referer)
(46.30.10.167,20191125110709,131,200,-)
  */
object LogStreamingApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("LogStreamingApp")//.setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(60))

    // 使用Kafka消费者API消费ylqdh中的数据
    val messages = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](ParamsConf.topic,ParamsConf.kafkaParams))

    // x.value获得数据里的值
    val logs = messages.map(x => x.value())

    // 步骤1. 清洗数据
    val cleanData = logs.map(line => {

      val infos = line.split("\t")
      // 筛选一行数据长度为5的行，其他的舍弃
      if (infos.length != 5) {
        // 如何跳过此条数据
      }
      val courseHtml = infos(2).split(" ")(1)
      var courseID = 0

      // 筛选课程是class的，不要learn、course等其他的
      if (courseHtml.contains("class")) {
        val classCou = courseHtml.split("/")(2)
        courseID = classCou.substring(0,classCou.lastIndexOf(".")).toInt
      }

      ClickLogCase(infos(0),DateUtil.parseToMinute(infos(1)),courseID,infos(3).toInt,infos(4))

    }).filter(clicklog => clicklog.courseID != 0)

//    cleanData.print()

    // 步骤2. 把清洗的数据按照hbase表结构，写入到hbase中
    // 需求1. 统计每天每个课程的访问量
    cleanData.map( x => {
      (x.time.substring(0,8)+"_"+x.courseID,1)  // 在所有数据中拿到日期和课程id拼接成的结构
    }).reduceByKey(_+_)   // wordcount操作，计算当天的count值
      .foreachRDD(rdd => {
      rdd.foreachPartition( partitionRecodes => {
        val list = new ListBuffer[CourseClickCountCase]
        partitionRecodes.foreach(pair => {
          list.append(CourseClickCountCase(pair._1,pair._2))
        })
        CourseClickCountDAO.save(list)
      })
    })

    // 需求2. 统计每天每个课程从不同搜索引擎过来的点击数
    cleanData.map( x => {
      /**
        *  https://www.baidu.com/s?wd=Storm实战 中拿到 www.baidu.com
        *
        */
      val url = x.referer.replaceAll("//","/")
      val splits = url.split("/")
      var host = ""

      // 过滤掉不符合的搜索引擎地址
      if (splits.length > 2) {
        host = splits(1)
      }
      (host,x.courseID,x.time.substring(0,8))
    }).filter(_._1 != "") // 过滤掉host为空的数据
      .map( x => {        // 把数据合并成 pairRDD
      (x._3+"_"+x._1+"_"+x._2 , 1)
    }).reduceByKey(_+_)   // wordcount操作，计算当天的count值
      .foreachRDD(rdd => {
      rdd.foreachPartition( partitionRecodes => {
        val list = new ListBuffer[CourseSearchCountCase]
        partitionRecodes.foreach(pair => {
          list.append(CourseSearchCountCase(pair._1,pair._2))
        })
        CourseSearchCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}