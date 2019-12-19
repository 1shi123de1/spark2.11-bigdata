package project.weblog.ylqdh.bigdata.streaming

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import project.imooc.ylqdh.bigdata.streaming.utils.ParamsConf
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs
import org.apache.kafka.common.TopicPartition

object StreamingOffsetApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StreamingOffsetApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    // 从MySQL中读取Kafka的offset
    DBs.setup()
    val fromOffset = DB.readOnly(implicit session=> {
      SQL("select * from kafka_offset").map(rs => {
        ( new TopicPartition(rs.string("topic"),rs.int("partition_id")),rs.long("untilOffset"))
      }).list().apply()
    }).toMap

    // 使用Kafka消费者API消费ylqdh 这个topic中的数据
    // 如果数据库中offset为空，则从头开始消费，如果不为空则从offset记录开始消费
    val messages = if (fromOffset.isEmpty) {
      println("从头开始消费...")
      KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String,String](ParamsConf.topic,ParamsConf.kafkaParams))
    } else {
      println("从已存在记录开始消费...")
      KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String,String](fromOffset.keys.toList,ParamsConf.kafkaParams,fromOffset)
      )
    }

    // 消费数据，消费完之后要把offset记录回到MySQL中
    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        println("当前RDD数据量为： "+rdd.count())
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd.foreachPartition { _ =>
          val o: OffsetRange = offsetRanges(TaskContext.getPartitionId())
          println(s"${o.topic},${o.partition},${o.fromOffset},${o.untilOffset}")

          // 开始处理事务，RDD的数据计算统计
          // 统计完成之后记录offset回MySQL，最后才结束事务，这样如果事务不成功，记录offset的操作也会回滚

          //记录offset到MySQL
          DB.localTx(implicit session => {
            SQL("replace into kafka_offset(group_id,topic,partition_id,fromOffset,untilOffset) values (?,?,?,?,?)")
              .bind(ParamsConf.groupId,o.topic,o.partition.toInt,o.fromOffset.toLong,o.untilOffset.toLong)
              .update().apply()
          })

          // 在这里再结束事务操作
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
