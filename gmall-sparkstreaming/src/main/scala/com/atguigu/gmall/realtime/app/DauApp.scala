package com.atguigu.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetManageUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


/**
 * 统计日活
 * Daily Active User
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    // 1) spark-streaming要消费到kafka
    val sparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]") //kafka中topic指定了4个分区
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 2) 通过kafka工具类获得kafka数据流
    val topic = "ODS_BASE_LOG"
    val groupId = "gmall_group"

    //----(a)从redis中读取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManageUtil.getOffset(topic, groupId)

    //----(b)通过偏移量从指定位置消费kafka数据
    var inputDs: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap.isEmpty) {
      // 首次读取，redis不存在偏移量，则从kafka最新位置获取
      inputDs = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    } else {
      // 如果有偏移量值，则按照偏移量值获取数据
      inputDs = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }
    // inputDs是ConsumerRecord对象的泛型是kafka中数据的key-value的数据类型,这里key没指定是null
    // inputDs.asInstanceOf[CanCommitOffsets].commitAsync(Array[OffsetRange])// 偏移手动量提交，提交到kafka的_consumer_offsets主题中。只能
    // 是InputDStream[ConsumerRecord[String, String]]格式。如果发生格式转换，offset信息会丢失，(不是很适用)

    //----(c) 在inputDs转换前获取偏移量的结束点
    var offsetRanges: Array[OffsetRange] = null // Driver
    val inputWithOffsetDs: DStream[ConsumerRecord[String, String]] = inputDs.transform( // 转换算子
      rdd => {
        // 把rdd强转为某个特质，利用特质的offsetRanges方法 得到偏移量结束点（offsetRanges）
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //Driver执行
        rdd
      }
    )

    // 3) 统计用户当日首次访问(1个用户一天只能为这个统计结果贡献一个数)
    // 1、可以通过判断日志中page栏位没有last_page_id属性来决定该页面是否为首次访问。
    // 2、可以通过启动日志来判断，是否首次访问

    //转换为方便操作的JsonObject
    val logJsonDs: DStream[JSONObject] = inputWithOffsetDs.map {
      record =>
        val log: String = record.value()
        val logJsonObj: JSONObject = JSON.parseObject(log)

        // 转换时间戳ts为方便操作的时间类型
        val ts: Long = logJsonObj.getLong("ts")
        val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateHourString: String = format.format(new Date(ts))
        val dt: String = dateHourString.split(" ")(0)
        val hr: String = dateHourString.split(" ")(1)
        logJsonObj.put("dt", dt)
        logJsonObj.put("hr", hr)
        logJsonObj
    }
    // 保留首次访问的信息，即page栏位没有last_page_id
    val firstPageJsonDs: DStream[JSONObject] = logJsonDs.filter {
      logJsonObj =>
        var isFirstPage = false
        val pageJsonObj: JSONObject = logJsonObj.getJSONObject("page")
        if (pageJsonObj != null) {
          val lastPageId: String = pageJsonObj.getString("last_page_id")
          if (lastPageId == null) {
            isFirstPage = true
          }
        }
        isFirstPage
    }
    //    firstPageJsonDs.print(1000)

    // 多用户的会话(开关浏览器理解为一次会话)次数变为日活-->(去重)
    // 1如何去重：本质来说就是一种识别  识别每条日志的主体(mid设备id)  对于当日来说是不是已经来过了
    // 2如何保存：redis/sparkStreaming#UpdateStateByKey->checkpoint/mysql
    //    UpdateStateByKey弊端，checkpoint文件会越来越大，容易臃肿，文件不可读(读不懂).   [mapStatByKey目前不稳定]
    //    redis set/string(bitMap)  key = dau:2021-02-27 value = mid(设备号)

    //所有首次访问的日志根据mid去重
    /*    val dauJsonDs: DStream[JSONObject] = firstPageJsonDs.filter {
          logJsonObj =>
            val jedis: Jedis = RedisUtil.getJedisClient()
            val dauKey = "dau:" + logJsonObj.getString("dt")
            val mid: String = logJsonObj.getJSONObject("common").getString("mid")
            // sadd命令添加成功返回1，添加失败有重复的返回0
            val long: lang.Long = jedis.sadd(dauKey, mid)
            jedis.close()
            if (long == 1L) {
              println("用户：" + mid + "首次访问 保留")
              true
            } else {
              println("用户：" + mid + "已经重复，去掉")
              false
            }
        }*/
    val dauJsonDs: DStream[JSONObject] = firstPageJsonDs.mapPartitions {
      logJsonObjIter => // 为相同分区中的数据处理可以共用一个连接
        //代码在Executor端执行，每个批次的一个分区执行一次，即每个批次，每个分区用一个连接
        val jedis: Jedis = RedisUtil.getJedisClient()
        val resultData = new ListBuffer[JSONObject]
        // 操作分区中数据
        for (logJsonObj <- logJsonObjIter) {
          val dauKey = "dau:" + logJsonObj.getString("dt")
          val mid: String = logJsonObj.getJSONObject("common").getString("mid")
          // sadd命令添加成功返回1，添加失败有重复的返回0
          val long: lang.Long = jedis.sadd(dauKey, mid)
          if (long == 1L) {
            println("用户：" + mid + "首次访问 保留")
            resultData.append(logJsonObj)
          } else {
            println("用户：" + mid + "已经重复，去掉")
          }
        }
        jedis.close()
        resultData.toIterator
    }

    dauJsonDs.print(1000)

    //----(d)此处把偏移量的结束点，更新到redis中，作为偏移量的提交
    dauJsonDs.foreachRDD { rdd =>
      rdd.foreachPartition { jsonObjIter =>
        for (jsonObj <- jsonObjIter) {
          print(jsonObj)
          // (a位置)Executor执行，每条数据执行一次
        }
        //(b位置)Executor执行，每个分区数据执行一次
      }
      // (c位置)Driver端执行，每个批次执行一次

      //把偏移量的结束点保存到redis中(Driver端执行),偏移量保存在Driver端
      OffsetManageUtil.saveOffset(topic, groupId, offsetRanges)
    }
    //(d位置) Driver执行，整个程序启动时执行一次


    ssc.start()
    ssc.awaitTermination()
  }
}
