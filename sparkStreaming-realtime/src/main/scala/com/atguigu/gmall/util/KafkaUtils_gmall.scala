package com.atguigu.gmall.util

import java.{io, util}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
  * @author fengwentao@changjing.ai
  * @date 2022/5/23 15:11
  * @Version 1.0.0
  * @Description TODO
  */
object KafkaUtils_gmall {
//kafka 消费配置
  private val consumerConfig = mutable.HashMap(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtils("kafka.bootstrap-servers"),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> "gmall_0519",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )

//  默认offsets位置消费
  def getKafkaDStream( ssc:StreamingContext, topic:String, groupId:String)={
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(topic),consumerConfig))
    kafkaDStream
  }

//  基于sparkstreaming消费,获取kafkadstream,使用指定的offset
  def getKafkaDStream(ssc:StreamingContext, topic:String, groupId: String, offsets:Map[TopicPartition,Long])={
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(topic),consumerConfig,offsets))
    kafkaDStream
  }

//创建生产者对象
  def createProducer() = {
    //配置生产者
    val producerConfig = new util.HashMap[String,AnyRef]()
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,PropertiesUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])
    producerConfig.put(ProducerConfig.ACKS_CONFIG,"all")
  //幂等操作
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true")

    val producer = new KafkaProducer[String,String](producerConfig)
    producer
  }

//  发送数据
  def send(topic:String,msg:String)={
    producer.send(new ProducerRecord[String,String](topic,msg))
  }

  //  发送数据, 按key进行分区
  def send(topic:String,key:String,msg:String)={
    producer.send(new ProducerRecord[String,String](topic,key,msg))
  }
  //  生产者对象
  val producer = createProducer()

//  关闭生产者对象
  def close()={
    if(producer != null){
      producer.close()
    }
  }
//  刷写,将缓冲区的数据刷写到磁盘
  def flush()={
    producer.flush()
  }

  def main(args: Array[String]): Unit = {

  }
}
