package com.flink.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

object KafkaConnectorApp {
  /**
   * 消费kafka
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkPoint 常用参数设置
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop000:9092")
    properties.setProperty("group.id", "test")

    val topic = "zzqq"

    val myConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)

    myConsumer.setStartFromEarliest()      // start from the earliest record possible
    //myConsumer.setStartFromLatest()        // start from the latest record
    //myConsumer.setStartFromTimestamp(1)  // start from specified epoch timestamp (milliseconds)
    //myConsumer.setStartFromGroupOffsets()  // the default behaviour

    //specify the exact offsets the consumer should start from for each partition:
    /*val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
    specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L)
    specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L)
    specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L)

    myConsumer.setStartFromSpecificOffsets(specificStartOffsets)*/

    val data = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties))
    data.print().setParallelism(1)

    env.execute("aaa")
  }

  /**
   * sink到kafka
   * @param args
   */
  def main2(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // checkPoint 常用参数设置
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    val stream: DataStream[String] = env.socketTextStream("localhost",9999)

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "192.168.33.10:9092")

    val myProducer = new FlinkKafkaProducer[String](
      "zzqq",                  // target topic
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),    // serialization schema
      properties,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE) // fault-tolerance

    stream.addSink(myProducer)

    env.execute("aaa")
  }

}
