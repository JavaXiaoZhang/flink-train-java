package com.flink.sink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode


object SinkScalaApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.fromCollection(1 to 10)
    val filePath = "./data/sink.txt"
    text.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(2)
    env.execute("SinkScalaApp")
  }
}
