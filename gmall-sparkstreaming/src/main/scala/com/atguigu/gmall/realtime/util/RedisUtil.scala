package com.atguigu.gmall.realtime.util

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  var jedisPool:JedisPool = null

  def getJedisClient() : Jedis= {
    if (jedisPool == null) {
      val config: Properties = PropertiesUtil.load("config.properties")
      val host: String = config.getProperty("redis.host")
      val port: String = config.getProperty("redis.port")

      val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig
      jedisPoolConfig.setMaxTotal(100)// 最大连接数
      jedisPoolConfig.setMaxIdle(20)// 最大空闲数
      jedisPoolConfig.setMinIdle(20)// 最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true)//忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000)// 等待时间
      jedisPoolConfig.setTestOnBorrow(true)// 每次获取连接都会测试
      jedisPool = new JedisPool(jedisPoolConfig,host,port.toInt)
    }
    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {
    val jedis: Jedis = getJedisClient()
    println(jedis.ping())
    jedis.close()
  }
}
