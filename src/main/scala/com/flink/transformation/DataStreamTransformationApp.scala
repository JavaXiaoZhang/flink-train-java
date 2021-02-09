package com.flink.transformation

import com.flink.source.CustomNonParallelSource
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object DataStreamTransformationApp {

  def filterFunction(env: StreamExecutionEnvironment) = {
    val data = env.addSource(new CustomNonParallelSource)
    data.map(x => x).filter(_ % 2 == 0).print().setParallelism(1)
  }

  def unionFunction(env: StreamExecutionEnvironment) = {
    val data1: DataStream[Long] = env.addSource(new CustomNonParallelSource)
    val data2: DataStream[Long] = env.addSource(new CustomNonParallelSource)
    data1.union(data2).print().setParallelism(1)
  }

  def splitFunction(env: StreamExecutionEnvironment) = {
    val data: DataStream[Long] = env.addSource(new CustomNonParallelSource)
    data.process(new ProcessFunction[Long, Any] {
      override def processElement(value: Long, ctx: ProcessFunction[Long, Any]#Context, out: Collector[Any]): Unit = {
        if (value < 10) {
          ctx.output(new OutputTag[Any]("small"), 0)
        } else {
          ctx.output(new OutputTag[Any]("big"), 999)
        }
      }
    }).getSideOutput (new OutputTag[Any]("small")).print().setParallelism(1)

    data.print().setParallelism(1)
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //filterFunction(env)
    splitFunction(env)
    env.execute("aa")
  }
}
