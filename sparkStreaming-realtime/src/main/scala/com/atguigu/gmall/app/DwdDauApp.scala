package com.atguigu.gmall.app

import java.lang
import java.sql.Date
import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.bean.{DauInfo, PageLog}
import com.atguigu.gmall.util.{BeanUtils_gmall, EsUtils_gmall, KafkaUtils_gmall, OffsetsUtils, RedisUtils_gmall}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * @author fengwentao@changjing.ai
  * @date 2022/5/24 21:55
  * @Version 1.0.0
  * @Description TODO
  */
/**
  * 日活宽表
  *
  * 1. 准备实时环境
  * 2. 从Redis中读取偏移量
  * 3. 从kafka中消费数据
  * 4. 提取偏移量结束点
  * 5. 处理数据
  *     5.1 转换数据结构
  *     5.2 去重
  *     5.3 维度关联
  * 6. 写入ES
  * 7. 提交offsets
  */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    //准备环境
    val spark: SparkSession = SparkSession.builder().master("local[2]").appName("DwdDauApp").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    //从redis读取偏移量
    val topicName = "DWD_PAGE_LOG_TOPIC"
    val groupId = "DWD_DAU_GROUP"
    val offsets: Map[TopicPartition, Long] = OffsetsUtils.readOffset(topicName, groupId)
    //消费kafka数据
    val kafkaDStream = if (offsets != null && offsets.size > 0) {
      KafkaUtils_gmall.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      KafkaUtils_gmall.getKafkaDStream(ssc, topicName, groupId)
    }
    // 提取数据的结束点
    var offsetRanges: Array[OffsetRange] = null
    kafkaDStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    //处理数据
    //结构转换
    val pageLogDStream: DStream[PageLog] = kafkaDStream.map(line => {
      val pageLog: PageLog = JSON.parseObject(line.value(), classOf[PageLog])
      pageLog
    })
    pageLogDStream.cache()
    pageLogDStream.foreachRDD(rdd => println("原始数据量(自我审查前):" + rdd.count()))
    //去重
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(p => p.last_page_id == null)
    filterDStream.cache()
    filterDStream.foreachRDD(rdd => {
      println("去重以后数据量(自我审查后):" + rdd.count())
      println("-----------------------------")
    })

    // 第三方审查:  通过redis将当日活跃的mid维护起来,自我审查后的每条数据需要到redis中进行比对去重
    // redis中如何维护日活状态
    // 类型:    set
    // key :    DAU:DATE
    // value :  mid的集合
    // 写入API: sadd
    // 读取API: smembers
    // 过期:  24小时
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
      pageLogIter => {
        val pageLogList: List[PageLog] = pageLogIter.toList
        println("第三方审查前:" + pageLogList.size)

        //        存储要的数据
        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
        val sdf = new SimpleDateFormat()
        val jedis: Jedis = RedisUtils_gmall.getJedisFromPool()

        for (pageLog <- pageLogList) {
          // 提取每条数据中的mid (我们日活的统计基于mid， 也可以基于uid)
          val mid = pageLog.mid
          //          获取日期
          val ts = pageLog.ts
          val dateStr: String = sdf.format(new Date(ts))
          val redisDauKey = s"DAU:${dateStr}"
          //redis的判断是否包含操作
          /*
          下面代码在分布式环境中，存在并发问题， 可能多个并行度同时进入到if中,导致最终保留多条同一个mid的数据.
          // list
          val mids: util.List[String] = jedis.lrange(redisDauKey, 0 ,-1)
          if(!mids.contains(mid)){
            jedis.lpush(redisDauKey , mid )
            pageLogs.append(pageLog)
          }
          // set
          val setMids: util.Set[String] = jedis.smembers(redisDauKey)
          if(!setMids.contains(mid)){
            jedis.sadd(redisDauKey,mid)
            pageLogs.append(pageLog)
          }

           */
          val isNew: lang.Long = jedis.sadd(redisDauKey, mid)
          if (isNew == 1L) {
            pageLogs.append(pageLog)
          }
        }
        jedis.close()
        println("第三方审查后:" + pageLogs.length)
        pageLogs.iterator
      }
    )

    //    维度关联
    val dauInfoDStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(pageLogIter => {
      val dauInfos: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val jedis: Jedis = RedisUtils_gmall.getJedisFromPool()
      for (pageLog <- pageLogIter) {
        val dauInfo = new DauInfo()
        //1. 将pagelog中以后的字段拷贝到DauInfo中
        //笨办法: 将pageLog中的每个字段的值挨个提取，赋值给dauInfo中对应的字段。
        //dauInfo.mid = pageLog.mid
        //好办法: 通过对象拷贝来完成.
        BeanUtils_gmall.copyProperties(pageLog, dauInfo)
        //2. 补充维度
        //2.1  用户信息维度
        val uid: String = pageLog.user_id
        val redisUidkey: String = s"DIM:USER_INFO:$uid"
        val userInfoJson: String = jedis.get(redisUidkey)
        val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
        //提取性别
        val gender: String = userInfoJsonObj.getString("gender")
        //提取生日
        val birthday: String = userInfoJsonObj.getString("birthday") // 1976-03-22
        //换算年龄
        val birthdayLd: LocalDate = LocalDate.parse(birthday)
        val nowLd: LocalDate = LocalDate.now()
        val period: Period = Period.between(birthdayLd, nowLd)
        val age: Int = period.getYears

        //补充到对象中
        dauInfo.user_gender = gender
        dauInfo.user_age = age.toString

        //2.2  地区信息维度
        // redis中:
        // 现在: DIM:BASE_PROVINCE:1
        // 之前: DIM:BASE_PROVINCE:110000
        val provinceID: String = dauInfo.province_id
        val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
        val provinceJson: String = jedis.get(redisProvinceKey)
        val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
        val provinceName: String = provinceJsonObj.getString("name")
        val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
        val province3166: String = provinceJsonObj.getString("iso_3166_2")
        val provinceAreaCode: String = provinceJsonObj.getString("area_code")

        //补充到对象中
        dauInfo.province_name = provinceName
        dauInfo.province_iso_code = provinceIsoCode
        dauInfo.province_3166_2 = province3166
        dauInfo.province_area_code = provinceAreaCode

        //2.3  日期字段处理
        val date = new Date(pageLog.ts)
        val dtHr: String = sdf.format(date)
        val dtHrArr: Array[String] = dtHr.split(" ")
        val dt: String = dtHrArr(0)
        val hr: String = dtHrArr(1).split(":")(0)
        //补充到对象中
        dauInfo.dt = dt
        dauInfo.hr = hr

        dauInfos.append(dauInfo)
      }
      jedis.close()
      dauInfos.iterator
    })

    //写入es
    //写入到OLAP中
    //按照天分割索引，通过索引模板控制mapping、settings、aliases等.
    //准备ES工具类
    dauInfoDStream.foreachRDD(rdd => {
      rdd.foreachPartition(
        dauInfoIter => {
//                        id      doc
          val docs: List[(String, DauInfo)] = dauInfoIter.map(dauInfo => (dauInfo.mid, dauInfo)).toList
          if (docs.size > 0) {
            // 索引名
            // 如果是真实的实时环境，直接获取当前日期即可.
            // 因为我们是模拟数据，会生成不同天的数据.
            // 从第一条数据中获取日期
            val head: (String, DauInfo) = docs.head
            val ts: Long = head._2.ts
            val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
            val dateStr: String = sdf.format(new Date(ts))
            val indexName: String = s"gmall_dau_info_$dateStr"
            //写入到ES中
            EsUtils_gmall.bulkSave(indexName, docs)
          }
        }
      )
      OffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
    })


    ssc.start()
    ssc.awaitTermination()


  }
}












