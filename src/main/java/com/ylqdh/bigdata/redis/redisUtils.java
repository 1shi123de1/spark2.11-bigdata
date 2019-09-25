package com.ylqdh.bigdata.redis;

/*
redis 工具类: 建议使用单例模式进行封装
 */

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class redisUtils {
    private static JedisPool jedisPool = null;

    private static final String HOST = "172.16.13.143";
    private static final int PORT = 6379;

    // jedis 连接池
    public static synchronized Jedis getJedis(){
        if(null == jedisPool){
            GenericObjectPoolConfig config = new JedisPoolConfig();
            config.setMaxIdle(10);
            config.setMaxTotal(100);
            config.setMaxWaitMillis(1000);
            config.setTestOnBorrow(true);

            jedisPool = new JedisPool(config,HOST,PORT);
        }

        return jedisPool.getResource();
    }
}
