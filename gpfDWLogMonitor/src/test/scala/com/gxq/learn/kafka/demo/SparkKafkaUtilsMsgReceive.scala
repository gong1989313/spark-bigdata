package com.gxq.learn.kafka.demo

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object SparkKafkaUtilsMsgReceive {
def main(args: Array[String]): Unit = {
      val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.2.13:9092,192.168.2.14:9092,192.168.2.15:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val conf = new SparkConf().setAppName("Spark Kafka Sample1")

    conf.setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(1))

    //val topics = List(("test", 1)).toMap
    val topics = Array("test")
    // Default Groupid
    val topicLines = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
      //KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group" ,topics)
      println("******************************")
      topicLines.print()

    ssc.start()

    ssc.awaitTermination()
  }
}