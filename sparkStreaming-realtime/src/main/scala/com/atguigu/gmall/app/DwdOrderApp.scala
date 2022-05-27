package com.atguigu.gmall.app

import java.time.{LocalDate, Period}
import java.util

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall
import com.atguigu.gmall.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall.util.{EsUtils_gmall, KafkaUtils_gmall, OffsetsUtils, RedisUtils_gmall}
import org.apache.kafka.clients.consumer.ConsumerRecord
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
  * @date 2022/5/26 10:23
  * @Version 1.0.0
  * @Description TODO
  */
/**
  * 订单宽表任务
  *
  * 1. 准备实时环境
  * 2. 从Redis中读取offset  * 2
  * 3. 从kakfa中消费数据 * 2
  * 4. 提取offset * 2
  * 5. 数据处理
  *    5.1 转换结构
  *    5.2 维度关联
  *    5.3 双流join
  * 6. 写入ES
  * 7. 提交offset * 2
  */
object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    //    1.准备实时环境
    val spark: SparkSession = SparkSession.builder().appName("DwdOrderApp").master("local[2]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    //    从redis中读取offset * 2
    //      order_info
    val orderInfoTopicName = "DWD_ORDER_INFO_I"
    val orderInfoGroup = "DWD_ORDER_INFO:GROUP"
    val orderInfo_offsets: Map[TopicPartition, Long] = OffsetsUtils.readOffset(orderInfoTopicName, orderInfoGroup)
    //      order_detail
    val orderDetailTopicName = "DWD_ORDER_DETAIL_I"
    val orderDetailGroup = "DWD_ORDER_DETAIL_GROUP"
    val orderDetail_offsets: Map[TopicPartition, Long] = OffsetsUtils.readOffset(orderDetailTopicName, orderDetailGroup)
    //    从kafka中消费数据 * 2
    val orderInfo_kafkaDStream = if (orderInfo_offsets != null && orderInfo_offsets.size > 0) {
      KafkaUtils_gmall.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup, orderInfo_offsets)
    } else {
      KafkaUtils_gmall.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup)
    }

    val orderDetail_kafkaDStream = if (orderDetail_offsets != null && orderDetail_offsets.size > 0) {
      KafkaUtils_gmall.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup, orderDetail_offsets)
    } else {
      KafkaUtils_gmall.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup)
    }

    //    提取offset * 2
    var orderInfo_offsetRanges: Array[OffsetRange] = null
    var orderDetail_offsetRanges: Array[OffsetRange] = null
    val orderInfo_Dstream: DStream[ConsumerRecord[String, String]] = orderInfo_kafkaDStream.transform(rdd => {
      orderInfo_offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val orderDetail_Dstream: DStream[ConsumerRecord[String, String]] = orderInfo_kafkaDStream.transform(rdd => {
      orderDetail_offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    //    数据处理
    //        转换结构
    val orderInfo_DS: DStream[OrderInfo] = orderInfo_Dstream.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      orderInfo
    })
    val orderDetail_DS: DStream[OrderDetail] = orderDetail_Dstream.map(record => {
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      orderDetail
    })

    //        维度关联
    val orderInfoDIMStream: DStream[OrderInfo] = orderInfo_DS.mapPartitions(
      orderInfoIter => {
        val orderInfos: List[OrderInfo] = orderInfoIter.toList
        val jedis: Jedis = RedisUtils_gmall.getJedisFromPool()
        for (orderInfo <- orderInfos) {
          //          关联用户数据
          val uid: Long = orderInfo.user_id
          val redisUserKey = s"MID:USER_INFO:$uid"
          val userinfoStr: String = jedis.get(redisUserKey)
          val userInfoOBJ: JSONObject = JSON.parseObject(userinfoStr)
          //          提取性别
          val gender: String = userInfoOBJ.getString("gender")
          //          提取生日
          val birthday: String = userInfoOBJ.getString("birthday")
          //          换算年龄
          val birthDay_LD: LocalDate = LocalDate.parse(birthday)
          val now: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthDay_LD, now)
          val ages: Int = period.getYears
          //          构建对象
          orderInfo.user_age = ages
          orderInfo.user_gender = gender

          //关联地区维度
          val provinceID: Long = orderInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)

          val provinceName: String = provinceJsonObj.getString("name")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")

          //补充到对象中
          orderInfo.province_name = provinceName
          orderInfo.province_area_code = provinceAreaCode
          orderInfo.province_3166_2_code = province3166
          orderInfo.province_iso_code = provinceIsoCode

          //处理日期字段
          val createTime: String = orderInfo.create_time
          val createDtHr: Array[String] = createTime.split(" ")
          val createDate: String = createDtHr(0)
          val createHr: String = createDtHr(1).split(":")(0)
          //补充到对象中
          orderInfo.create_date = createDate
          orderInfo.create_hour = createHr
        }
        jedis.close()
        orderInfos.iterator
      }
    )
    //    双流join
    val idOrderInfoDIM: DStream[(Long, OrderInfo)] = orderInfoDIMStream.map(x => (x.id, x))
    val idOrderDetailDIM: DStream[(Long, OrderDetail)] = orderDetail_DS.map(x => (x.order_id, x))
    //    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = idOrderInfoDIM.fullOuterJoin(idOrderDetailDIM)
    // 解决:
    //  1. 扩大采集周期 ， 治标不治本
    //  2. 使用窗口,治标不治本 , 还要考虑数据去重 、 Spark状态的缺点
    //  3. 首先使用fullOuterJoin,保证join成功或者没有成功的数据都出现到结果中.
    //     让双方都多两步操作, 到缓存中找对的人， 把自己写到缓存中
    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = idOrderInfoDIM.fullOuterJoin(idOrderDetailDIM)
    val orderWide_DS: DStream[OrderWide] = orderJoinDStream.mapPartitions(orderJoinIter => {
      val jedis: Jedis = RedisUtils_gmall.getJedisFromPool()
      val orderWides: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
      for ((orderid, (orderinfoOp, orderDetailOp)) <- orderJoinIter) {
        //        orderInfo 有 orderDetail 有
        if (orderinfoOp.isDefined) {
          val orderInfo: OrderInfo = orderinfoOp.get
          if (orderDetailOp.isDefined) {
            val orderDetail: OrderDetail = orderDetailOp.get
            //组装成orderWide
            val orderWide = new OrderWide(orderInfo, orderDetail)
            orderWides.append(orderWide)
          }

          //orderinfo 有 orderdetail 没有
          //orderInfo写缓存
          // 类型:  string
          // key :   ORDERJOIN:ORDER_INFO:ID
          // value :  json
          // 写入API:  set
          // 读取API:  get
          // 是否过期: 24小时
          val redisOrderInfoKey = s"ORDERJOIN:ORDER_INFO:${orderInfo.id}"
          jedis.setex(redisOrderInfoKey, 24 * 3600, JSON.toJSONString(orderInfo, new SerializeConfig(true)))
          //orderdetail读缓存
          val redisOrderDetailKey = s"ORDERJOIN:ORDER_DETAIL:${orderInfo.id}"
          val orderDetails: util.Set[String] = jedis.smembers(redisOrderDetailKey)
          if (orderDetails != null && orderDetails.size() > 0) {
            import scala.collection.JavaConverters._
            for (orderDetailJson <- orderDetails.asScala) {
              val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
              val wide = new OrderWide(orderInfo, orderDetail)
              orderWides.append(wide)
            }
          }
        } else {
          //          orderinfo没有 orderDetail you
          val orderDetail: OrderDetail = orderDetailOp.get
          val redisOrderInfolKey = s"ORDERJOIN:ORDER_INFO:${orderDetail.order_id}"
          val orderInfoJson: String = jedis.get(redisOrderInfolKey)
          if (orderInfoJson != null && orderInfoJson.size > 0) {
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            val wide = new OrderWide(orderInfo, orderDetail)
            orderWides.append(wide)
          } else {
            val redisOrderDetaillKey = s"ORDERJOIN:ORDER_DETAIL:${orderDetail.order_id}"
            jedis.sadd(redisOrderDetaillKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
            jedis.expire(redisOrderDetaillKey, 24 * 3600)
          }
        }
      }
      jedis.close()
      orderWides.iterator
    })

    //    写入es
    orderWide_DS.foreachRDD(rdd=>{
      rdd.foreachPartition(orderWideIter=>{
        val orderWides: List[(String, OrderWide)] = orderWideIter.map(orderWide=>(orderWide.detail_id.toString, orderWide)).toList
        if(orderWides.size > 0){
          val head: (String, OrderWide) = orderWides.head
          val date = head._2.create_date
//          索引名
          val indexName = s"gmall_order_wide_${date}"
//          写入es
          EsUtils_gmall.bulkSave(indexName,orderWides)
        }
      })
      //    提交offset
      OffsetsUtils.saveOffset(orderInfoTopicName,orderInfoGroup,orderInfo_offsetRanges)
      OffsetsUtils.saveOffset(orderDetailTopicName,orderDetailGroup,orderDetail_offsetRanges)
    }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
