package com.flink.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowsAppScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)
    text.flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      //.timeWindow(Time.seconds(5))
      .timeWindow(Time.seconds(10),Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)
    env.execute("aaa")
  }
}