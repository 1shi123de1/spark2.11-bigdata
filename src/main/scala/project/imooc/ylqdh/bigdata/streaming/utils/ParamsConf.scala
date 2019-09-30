package project.imooc.ylqdh.bigdata.streaming.utils

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer

/*
  项目参数配置读取类

  配置统一管理
 */
object ParamsConf {

  private lazy val config = ConfigFactory.load()

  val topic = config.getString("kafka.topic").split(",")
  val groupId = config.getString("kafka.group.id")
  val brokers = config.getString("kafka.broker.list")

  val redisHost = config.getString("redis.host")
  val redisDB = config.getInt("redis.db")

  val kafkaParams = Map[String,Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupId,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false:java.lang.Boolean)
  )


  def main(args: Array[String]): Unit = {
    println(ParamsConf.topic)
  }
}
