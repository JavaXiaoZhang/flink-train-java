package com.flink.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object DataStreamSourceApp {

  def nonParallelSourceFunction(env: StreamExecutionEnvironment) = {
    val data: DataStream[Long] = env.addSource(new CustomNonParallelSource).setParallelism(1)
    data.print().setParallelism(1)
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // socketFunction(env)
    nonParallelSourceFunction(env)
    env.execute("aaa")
  }

  def socketFunction(env:StreamExecutionEnvironment): Unit ={
    val data = env.socketTextStream("localhost", 9999)
    data.print().setParallelism(1)
  }
}
