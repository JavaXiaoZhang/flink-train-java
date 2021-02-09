package com.flink.counter

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

object ScalaCounter {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromCollection(10 to 20)

    // 单线程运行时，结果正确；多线程运行时，结果错误
    /*data.map(new RichMapFunction[Int,Int] {
      var counter = 0
      override def map(value: Int): Int = {
        counter += 1
        counter
      }
    }).setParallelism(2).print()*/

    // 多线程环境下，统计计数
    data.map(new RichMapFunction[Int,Int] {
      // 1.定义计数器
      var counter = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("scala-counter",counter)
        super.open(parameters)
      }

      override def map(value: Int): Int = {
        counter.add(1)
        value
      }
    }).writeAsText("./data/counter-out",WriteMode.OVERWRITE).setParallelism(5)

    val jobExecutionResult = env.execute("ScalaCounter")
    val value = jobExecutionResult.getAccumulatorResult[Int]("scala-counter")
    println("counter:"+value)
  }

}
