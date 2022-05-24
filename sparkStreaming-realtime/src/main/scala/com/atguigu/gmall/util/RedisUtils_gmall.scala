package com.atguigu.gmall.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/5/23 20:40
  * @Version 1.0.0
  * @Description TODO
  */
object RedisUtils_gmall {

  var jedisPool:JedisPool = null

  def getJedisFromPool(): Jedis = {
    if(jedisPool == null){
//      链接redis
      val config = new JedisPoolConfig()
      config.setMaxTotal(100)
      config.setMaxIdle(20)
      config.setMinIdle(20)
      config.setBlockWhenExhausted(true)
      config.setMaxWaitMillis(5000)
      config.setTestOnBorrow(true)

      val host = PropertiesUtils(MyConfig.REDIS_HOST)
      val port = PropertiesUtils(MyConfig.REDIS_PORT).toInt

      jedisPool = new JedisPool(config,host,port)
    }
    jedisPool.getResource()
  }

  def main(args: Array[String]): Unit = {
    println(RedisUtils_gmall.getJedisFromPool())
    println(RedisUtils_gmall.getJedisFromPool())
  }
}

