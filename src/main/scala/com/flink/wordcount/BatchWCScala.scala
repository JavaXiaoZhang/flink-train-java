package com.flink.wordcount

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * 批处理
 */
object BatchWCScala {
  def main(args: Array[String]): Unit = {
    val dataPath = "./data/data.txt"
    // 1. 获取运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    ExecutionEnvironment.createLocalEnvironment()
    // 2. 读数据
    val textFile = env.readTextFile(dataPath)

    // 3. transform
    import org.apache.flink.api.scala._
    textFile.flatMap(_.toLowerCase.split(" ")).filter(_.nonEmpty).map((_, 1)).groupBy(0).sum(1).print()
  }
}
