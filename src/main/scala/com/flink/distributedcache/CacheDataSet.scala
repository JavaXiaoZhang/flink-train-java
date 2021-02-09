package com.flink.distributedcache

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object CacheDataSet {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val toBoardCast: DataSet[String] = env.fromElements("a", "b")

    val data = env.fromElements("java", "scala")
    data.map(new RichMapFunction[String,String] {

      override def open(parameters: Configuration): Unit = {
        val list: util.List[String] = getRuntimeContext.getBroadcastVariable("bc-set")
        import scala.collection.JavaConverters._
        for (ele <- list.asScala){
          println(ele)
        }
      }

      override def map(value: String): String = {
        value
      }
    }).withBroadcastSet(toBoardCast,"bc-set").print()
  }
}
