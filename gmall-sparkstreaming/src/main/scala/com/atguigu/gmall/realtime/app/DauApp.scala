package com.atguigu.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


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
    val inputDs: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    // inputDs是ConsumerRecord对象的泛型是kafka中数据的key-value的数据类型,这里key没指定是null
    //    inputDs.map(_.value()).print(1000)

    // 3) 统计用户当日首次访问(1个用户一天只能为这个统计结果贡献一个数)
    // 1、可以通过判断日志中page栏位没有last_page_id属性来决定该页面是否为首次访问。
    // 2、可以通过启动日志来判断，是否首次访问

    //转换为方便操作的JsonObject
    val logJsonDs: DStream[JSONObject] = inputDs.map {
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
    println("xxx")
    firstPageJsonDs.transform(
      rdd=>{
        println("111111111111111111111111111111111111")
        println("过滤前" + rdd.count())
        rdd.map{
          a=>1
        }
      }
    )

    //所有首次访问的日志根据mid去重
    val dauJsonDs: DStream[JSONObject] = firstPageJsonDs.filter {
      logJsonObj =>
        val jedis = new Jedis("hadoop102", 6379)
        val dauKey = "dau:" + logJsonObj.getString("dt")
        val mid: String = logJsonObj.getJSONObject("common").getString("mid")
        // sadd命令添加成功返回1，添加失败有重复的返回0
        val long: lang.Long = jedis.sadd(dauKey, mid)
        if (long == 1L) {
          println("用户："+mid+"首次访问 保留")
          true
        } else {
          println("用户："+mid+"已经重复，去掉")
          false
        }
    }

    dauJsonDs.print(1000)

    ssc.start()
    ssc.awaitTermination()
  }
}
