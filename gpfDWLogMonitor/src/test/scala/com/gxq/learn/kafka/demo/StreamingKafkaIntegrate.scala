package com.gxq.learn.kafka.demo

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object StreamingKafkaIntegrate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kafkaTest").setMaster("local")
    val streamingContext = new StreamingContext(conf, Seconds(1))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.2.13:9092,192.168.2.14:9092,192.168.2.15:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    //val result =  stream.map(record => (record.key, record.value))
   // stream.map(record => (record.value().toString()))
    stream.map(record=>(record.value().toString)).print
    println("-----------------------------")

    streamingContext.start()

    streamingContext.awaitTermination()
  }
}