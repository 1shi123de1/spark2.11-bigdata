package project.imooc.ylqdh.bigdata.streaming.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import project.imooc.ylqdh.bigdata.streaming.utils.{ParamsConf, RedisPool}

/*
    统计每天付费成功的总订单数
    统计每天付费成功的总订单金额
   */
object StreamingApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingApp")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    // kafka 消费数据
    val stream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](ParamsConf.topic,ParamsConf.kafkaParams)
    )

    stream.map(x => x.value()).print()

    // 数据拿到之后，就可以进行统计分析了
    stream.foreachRDD(rdd => {
      val data = rdd.map(x=>JSON.parseObject(x.value()))

      data.cache()

      // 每天付费成功的总订单数
      data.map(x => {
        val time = x.getString("time")
        val day = time.substring(0,8)
        val flag = x.getString("flag")
        val flagResult = if(flag == "1") 1 else 0
        (day,flagResult)
      }).reduceByKey(_+_).foreachPartition(partition => {
        val jedis = RedisPool.getJedis()
        partition.foreach(x => {
          jedis.incrBy("ImoocSalesCount-"+x._1,x._2)
        })
      })

      // 每天付费成功的总订单金额
      data.map(x => {
        val time = x.getString("time")
        val day = time.substring(0,8)
        val flag = x.getString("flag")
        val fee = if(flag == "1") x.getString("fee").toLong else 0
        (day,fee)
      }).reduceByKey(_+_).foreachPartition(partition => {
        val jedis = RedisPool.getJedis()
        partition.foreach(x => {
          jedis.incrBy("ImoocFeeCount-"+x._1,x._2)
        })
      })


      data.unpersist(true)
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
