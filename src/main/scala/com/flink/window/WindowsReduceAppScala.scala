package com.flink.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowsReduceAppScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)
    text.flatMap(_.split(","))
      .map(x=>(1,x.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce((v1,v2)=>{
        println(v1+"+"+v2)
        (v1._1,v1._2+v2._2)
      })
      .print().setParallelism(1)
    env.execute("aaa")
  }
}
