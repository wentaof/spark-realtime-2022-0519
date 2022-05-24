package com.atguigu.gmall.app

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.util.{KafkaUtils_gmall, OffsetsUtils, RedisUtils_gmall}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * @author fengwentao@changjing.ai
  * @date 2022/5/24 14:42
  * @Version 1.0.0
  * @Description TODO
  */
/**
  * 业务数据消费分流
  *
  * 1. 准备实时环境
  *
  * 2. 从redis中读取偏移量
  *
  * 3. 从kafka中消费数据
  *
  * 4. 提取偏移量结束点
  *
  * 5. 数据处理
  *     5.1 转换数据结构
  *     5.2 分流
  * 事实数据 => Kafka
  * 维度数据 => Redis
  * 6. flush Kafka的缓冲区
  *
  * 7. 提交offset
  *
  *
  */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    //    1. 准备实时环境
    val spark: SparkSession = SparkSession.builder().appName("OdsBaseDbApp").master("local[2]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    val topicName = "ODS_BASE_DB_M"
    val groupId = "ODS_BASE_DB_GROUP"
    //    2.从redis中读取偏移量
    val offsets: Map[TopicPartition, Long] = OffsetsUtils.readOffset(topicName, groupId)
    //   3. 从kafka中消费数据
    val KafkaDStream = if (offsets != null && offsets.size > 0) {
      KafkaUtils_gmall.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      KafkaUtils_gmall.getKafkaDStream(ssc, topicName, groupId)
    }
    //    4.提取这次数据流的偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = KafkaDStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    //    println("offsetRanges:",offsetRanges)
    //    5.数据处理
    //      转换数据结构
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(record => {
      val jsonStr: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonStr)
      jsonObj
    })
    //    jsonObjDStream.print(10)
    //      分流
    //Redis连接写到哪里???
    // foreachRDD外面:  driver  ，连接对象不能序列化，不能传输
    // foreachRDD里面, foreachPartition外面 : driver  ，连接对象不能序列化，不能传输
    // foreachPartition里面 , 循环外面：executor ， 每分区数据开启一个连接，用完关闭.
    // foreachPartition里面,循环里面:  executor ， 每条数据开启一个连接，用完关闭， 太频繁。
    jsonObjDStream.foreachRDD(
      rdd => {
        val redisFactKey = "FACT:TABLES"
        val redisDIMKey = "DIM:TABLES"
        val jedis: Jedis = RedisUtils_gmall.getJedisFromPool()
        //        获取事实表名
        val factTables: util.Set[String] = jedis.smembers(redisFactKey)
        val factTablesBC: Broadcast[util.Set[String]] = sc.broadcast(factTables)
        //        获取维度表名
        val dimTables: util.Set[String] = jedis.smembers(redisDIMKey)
        val dimTablesBC: Broadcast[util.Set[String]] = sc.broadcast(dimTables)
        jedis.close()

        //遍历rdd
        rdd.foreachPartition(jsonObjIter=>{
//          开启redis连接
          val jedis: Jedis = RedisUtils_gmall.getJedisFromPool()
          for (jsonObj <- jsonObjIter) {
//            提取操作类型
            val operType: String = jsonObj.getString("type")
            val opValue = operType match {
              case "insert" => "I"
              case "bootstrap-insert" => "I"
              case "update" => "U"
              case "delete" => "D"
              case _ => null
            }
//            判断数据类型
            if(opValue != null ){
//              提取表名
              val tableName: String = jsonObj.getString("table")
//              事实表数据 存到kafka
              if(factTablesBC.value.contains(tableName)){
                val data = jsonObj.getString("data")
                val dwdTopicName = f"DWD_${tableName}_${opValue}"
                KafkaUtils_gmall.send(dwdTopicName,data)

//                模拟数据延迟
                if(tableName.equals("order_detail")){
                  Thread.sleep(200)
                }
              }
//              维度表数据 存到redis
              if(dimTablesBC.value.contains(tableName)){
                //维度数据
                // 类型 : string  hash
                //        hash ： 整个表存成一个hash。 要考虑目前数据量大小和将来数据量增长问题 及 高频访问问题.
                //        hash :  一条数据存成一个hash.
                //        String : 一条数据存成一个jsonString.
                // key :  DIM:表名:ID
                // value : 整条数据的jsonString
                // 写入API: set
                // 读取API: get
                // 过期:  不过期
                //提取数据中的id
                val dataObj: JSONObject = jsonObj.getJSONObject("data")
                val id = dataObj.getString("id")
                val redisKey = s"DIM:${tableName.toUpperCase}:${id}"
                jedis.set(redisKey,dataObj.toString())
              }
            }
          }
//          关闭redis
            jedis.close()
//          将kafka数据刷到缓冲区
          KafkaUtils_gmall.flush()
        })
        //提交偏移量到redis
        OffsetsUtils.saveOffset(topicName,groupId,offsetRanges)
      }

    )

    //    等待结束
    ssc.start()
    ssc.awaitTermination()
  }
}






















