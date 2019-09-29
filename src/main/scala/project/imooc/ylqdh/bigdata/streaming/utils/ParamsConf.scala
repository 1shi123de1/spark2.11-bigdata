package project.imooc.ylqdh.bigdata.streaming.utils

import com.typesafe.config.ConfigFactory

/*
  项目参数配置读取类

  配置统一管理
 */
object ParamsConf {

  private lazy val config = ConfigFactory.load()

  val topic = config.getString("kafka.topic")
  val groupId = config.getString("kafka.group.id")
  val brokers = config.getString("kafka.broker.list")

  val redisHost = config.getString("redis.host")
  val redisDB = config.getString("redis.db")



  def main(args: Array[String]): Unit = {
    println(ParamsConf.topic)
  }
}
