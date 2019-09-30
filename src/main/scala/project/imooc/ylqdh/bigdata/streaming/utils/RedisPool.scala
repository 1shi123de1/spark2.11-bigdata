package project.imooc.ylqdh.bigdata.streaming.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisPool {

  val poolConfig = new GenericObjectPoolConfig()
  poolConfig.setMaxIdle(10)
  poolConfig.setMaxTotal(1000)

  private lazy val jedisPool = new JedisPool(poolConfig,ParamsConf.redisHost)

  def getJedis() = {
    val jedis = jedisPool.getResource
    jedis.select(ParamsConf.redisDB)
    jedis
  }

}
