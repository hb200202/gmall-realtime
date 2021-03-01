package com.atguigu.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

object OffsetManageUtil {

  def getOffset(topic: String, consumeGroupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = RedisUtil.getJedisClient()
    val key = "offset:" + topic + ":" + consumeGroupId
    // hash存储在redis key = offset:topic:groupId  value = (partition,offset)
    val javaMap: util.Map[String, String] = jedis.hgetAll(key)
    jedis.close()

    import collection.JavaConverters._ // 隐式转换
    // 将java.util.Map转为scala中的Map
    val scalaMap: mutable.Map[String, String] = javaMap.asScala

    val partitionToLong: mutable.Map[TopicPartition, Long] = scalaMap.map {
      case (partition, offset) => {
        val topicPartition: TopicPartition = new TopicPartition(topic, partition.toInt)
        println("偏移量读取：分区：" + partition + "偏移量起始点:" + offset)
        (topicPartition, offset.toLong)
      }
    }
    // 可变集合转不可变集合
    partitionToLong.toMap
  }

  /**
   * @param topic          主题
   * @param consumeGroupId 消费者组
   * @param offsetRanges   每个分区的偏移量结束点
   */
  def saveOffset(topic: String, consumeGroupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    val jedis: Jedis = RedisUtil.getJedisClient()
    val key = "offset:" + topic + ":" + consumeGroupId
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    for (offsetRange <- offsetRanges) {
      val untilOffset: Long = offsetRange.untilOffset // 偏移量结束点
      val partition: Int = offsetRange.partition
      println("偏移量写入：分区：" + partition + "偏移量结束点:" + untilOffset)
      offsetMap.put(partition.toString, untilOffset.toString)
    }
    jedis.hset(key, offsetMap)
    jedis.close()
  }

}
