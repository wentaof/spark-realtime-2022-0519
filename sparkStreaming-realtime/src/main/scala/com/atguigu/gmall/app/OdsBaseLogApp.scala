package com.atguigu.gmall.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.atguigu.gmall.util.{KafkaUtils_gmall, OffsetsUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author fengwentao@changjing.ai
  * @date 2022/5/23 16:12
  * @Version 1.0.0
  * @Description TODO
  *              日志数据的消费分流
  * 1. 准备实时处理环境 StreamingContext
  *
  * 2. 从Kafka中消费数据
  *
  * 3. 处理数据
  *     3.1 转换数据结构
  *              专用结构  Bean
  *              通用结构  Map JsonObject
  *     3.2 分流
  *
  * 4. 写出到DWD层
  */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    //    1. 准备实时处理环境 StreamingContext
    val spark: SparkSession = SparkSession.builder().master("local[2]").appName("OdsBaseLogApp").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    //    2. 从Kafka中消费数据
    val ODS_TOPIC = "ODS_BASE_LOG"
    val ODS_GROUP_ID = "ODS_BASE_LOG_GROUP"
    //todo 在redis中获取offset ,指定offset进行消费
    val offsets: Map[TopicPartition, Long] = OffsetsUtils.readOffset(ODS_TOPIC, ODS_GROUP_ID)

    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = if (offsets != null && offsets.size > 0) {
      //      指定偏移量消费
      KafkaUtils_gmall.getKafkaDStream(ssc, ODS_TOPIC, ODS_GROUP_ID, offsets)
    } else {
      //      默认消费
      KafkaUtils_gmall.getKafkaDStream(ssc, ODS_TOPIC, ODS_GROUP_ID)
    }
//    println(kafkaDStream)
    //    3. 处理数据
    //    3.1 转换数据结构
    //          专用结构  Bean
    //          通用结构  Map JsonObject
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDstream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })


    val jsonObjDstream: DStream[JSONObject] = offsetRangesDstream.map(x => {
      val log: String = x.value()
      val jsonObj: JSONObject = JSON.parseObject(log)
      jsonObj
    })
    //    jsonObjDstream.print()

    //    3.2 分流
    val DWD_PAGE_LOG_TOPIC: String = "DWD_PAGE_LOG_TOPIC" // 页面访问
    val DWD_PAGE_DISPLAY_TOPIC: String = "DWD_PAGE_DISPLAY_TOPIC" //页面曝光
    val DWD_PAGE_ACTION_TOPIC: String = "DWD_PAGE_ACTION_TOPIC" //页面事件
    val DWD_START_LOG_TOPIC: String = "DWD_START_LOG_TOPIC" // 启动数据
    val DWD_ERROR_LOG_TOPIC: String = "DWD_ERROR_LOG_TOPIC" // 错误数据

    //分流规则:
    // 错误数据: 不做任何的拆分， 只要包含错误字段，直接整条数据发送到对应的topic
    // 页面数据: 拆分成页面访问， 曝光， 事件 分别发送到对应的topic
    // 启动数据: 发动到对应的topic

    //    4. 写出到DWD层
    jsonObjDstream.foreachRDD(rdd => {
      rdd.foreachPartition(jsonObjIter => {
        for (jsonObj <- jsonObjIter) {
          val errJsonObj: JSONObject = jsonObj.getJSONObject("err")
          if (errJsonObj != null) {
            //            错误数据
            KafkaUtils_gmall.send(DWD_ERROR_LOG_TOPIC, errJsonObj.toString)
          } else {
            // 提取公共字段
            val commonObj: JSONObject = jsonObj.getJSONObject("common")
            val ar: String = commonObj.getString("ar")
            val uid: String = commonObj.getString("uid")
            val os: String = commonObj.getString("os")
            val ch: String = commonObj.getString("ch")
            val isNew: String = commonObj.getString("is_new")
            val md: String = commonObj.getString("md")
            val mid: String = commonObj.getString("mid")
            val vc: String = commonObj.getString("vc")
            val ba: String = commonObj.getString("ba")
            //提取时间戳
            val ts: Long = jsonObj.getLong("ts")
            // 页面数据
            val pageObj: JSONObject = jsonObj.getJSONObject("page")
            if (pageObj != null) {
              //提取page字段
              val pageId: String = pageObj.getString("page_id")
              val pageItem: String = pageObj.getString("item")
              val pageItemType: String = pageObj.getString("item_type")
              val duringTime: Long = pageObj.getLong("during_time")
              val lastPageId: String = pageObj.getString("last_page_id")
              val sourceType: String = pageObj.getString("source_type")
              //封装成PageLog
              var pageLog = PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)
              //发送到DWD_PAGE_LOG_TOPIC
              KafkaUtils_gmall.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

              //提取曝光数据
              val displaysJsonArr: JSONArray = jsonObj.getJSONArray("displays")
              if (displaysJsonArr != null && displaysJsonArr.size() > 0) {
                for (i <- 0 until displaysJsonArr.size()) {
                  //循环拿到每个曝光
                  val displayObj: JSONObject = displaysJsonArr.getJSONObject(i)
                  //提取曝光字段
                  val displayType: String = displayObj.getString("display_type")
                  val displayItem: String = displayObj.getString("item")
                  val displayItemType: String = displayObj.getString("item_type")
                  val posId: String = displayObj.getString("pos_id")
                  val order: String = displayObj.getString("order")

                  //封装成PageDisplayLog
                  val pageDisplayLog = PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, displayType, displayItem, displayItemType, order, posId, ts)
                  // 写到 DWD_PAGE_DISPLAY_TOPIC
                  KafkaUtils_gmall.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))

                }
              }
              // 提取事件数据
              val actionJsonArr: JSONArray = jsonObj.getJSONArray("actions")
              if (actionJsonArr != null && actionJsonArr.size() > 0) {
                for (i <- 0 until (actionJsonArr.size())) {
                  val actionObj: JSONObject = actionJsonArr.getJSONObject(i)
                  //                  提取字段
                  val actionItem = actionObj.getString("item")
                  val actionId = actionObj.getString("action_id")
                  val actionItemType = actionObj.getString("item_type")
                  val actionTs = actionObj.getLong("ts")
                  val pageActionLog = PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, actionId, actionItem, actionItemType, actionTs, ts)
                  KafkaUtils_gmall.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                }
              }

              //              提取启动数据
              val startJsonObj: JSONObject = jsonObj.getJSONObject("start")
              if (startJsonObj != null) {
                //提取字段
                val entry: String = startJsonObj.getString("entry")
                val loadingTime: Long = startJsonObj.getLong("loading_time")
                val openAdId: String = startJsonObj.getString("open_ad_id")
                val openAdMs: Long = startJsonObj.getLong("open_ad_ms")
                val openAdSkipMs: Long = startJsonObj.getLong("open_ad_skip_ms")

                //封装StartLog
                var startLog = StartLog(mid, uid, ar, ch, isNew, md, os, vc, ba, entry, openAdId, loadingTime, openAdMs, openAdSkipMs, ts)
                //写出DWD_START_LOG_TOPIC
                KafkaUtils_gmall.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))

              }
            }
          }
          KafkaUtils_gmall.flush()
        }
//         foreach里面 executor中执行,一条数据执行一次
      }
      )
//      补充
//      foreachRDD里边,driver中执行,一个批次执行一次
//      提交偏移量
      OffsetsUtils.saveOffset(ODS_TOPIC,ODS_GROUP_ID,offsetRanges)
    })



    ssc.start()
    ssc.awaitTermination()
  }
}
