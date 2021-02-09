package com.flink.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 流处理
 */
object StreamingWCScalaApp {
  /**
   * keyBy和sum 通过Tuple传参方式指定index
   * @param args
   */
  def main2(args: Array[String]): Unit = {
    var port = 9999

    /*val parameterTool = ParameterTool.fromArgs(args)
    port = parameterTool.getInt("port")*/

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", port)
    import org.apache.flink.api.scala._
    text.flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("StreamingWCScalaApp")
  }

  /**
   * keyBy和sum 通过自定义类传参方式指定field名称
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)
    import org.apache.flink.api.scala._
    text.flatMap(_.split(","))
      .map(WordCount(_,1))
      //.keyBy("word")
      .keyBy(_.word) // keySelector方式
      .timeWindow(Time.seconds(5))
      .sum("count")
      .print()
      .setParallelism(1)

    env.execute("StreamingWCScalaApp")
  }

  case class WordCount(word:String,count:Int)

  /*class WordCount(xword:String,xcount:Int){
    var word = xword
    var count =xcount
  }*/
}
