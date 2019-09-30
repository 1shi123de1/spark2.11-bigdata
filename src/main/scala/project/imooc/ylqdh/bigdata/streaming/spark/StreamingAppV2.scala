package project.imooc.ylqdh.bigdata.streaming.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import project.imooc.ylqdh.bigdata.streaming.utils.{ParamsConf, RedisPool}

/*
  统计每小时付费的订单数&订单总额
  或者每分钟
 */
object StreamingAppV2 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingAppV2")
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
        .map(x => {
          val flag = x.getString("flag")
          val fee = x.getLong("fee")
          val time = x.getString("time")

          val day = time.substring(0,8)
          val hour = time.substring(8,10)
          val minute = time.substring(10,12)

          val success:(Long,Long) = if (flag == "1") (1,fee) else (0,0)

          /*
            把上面的数据规整为一个数据结构
            day,hour,minute 分别是时间粒度
           */
          (day,hour,minute,List[Long](1,success._1,success._2))
        })

      // day
      data.map(x => (x._1,x._4))
        .reduceByKey((a,b) => {
          a.zip(b).map(x => x._1 + x._2)
        }).foreachPartition(partition => {
        val jedis = RedisPool.getJedis()
        partition.foreach(x => {
          jedis.hincrBy("Imooc-"+x._1,"total",x._2(0))
          jedis.hincrBy("Imooc-"+x._1,"success",x._2(1))
          jedis.hincrBy("Imooc-"+x._1,"fee",x._2(2))
        })
      })

      // hour
      data.map(x => ((x._1,x._2),x._4))
        .reduceByKey((a,b) => {
          a.zip(b).map(x => x._1 + x._2)
        }).foreachPartition(partition => {
        val jedis = RedisPool.getJedis()
        partition.foreach(x => {
          jedis.hincrBy("Imooc-"+x._1._1,"total"+x._1._2,x._2(0))
          jedis.hincrBy("Imooc-"+x._1._1,"success"+x._1._2,x._2(1))
          jedis.hincrBy("Imooc-"+x._1._1,"fee"+x._1._2,x._2(2))
        })
      })

      })


    ssc.start()
    ssc.awaitTermination()
  }

}
