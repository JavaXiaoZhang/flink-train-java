package com.flink.distributedcache

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

/**
 * 数据本地化
 */
object CacheFIle {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.registerCachedFile("./data/data.txt","ds-file")
    val data = env.fromElements("java", "scala")

    data.map(new RichMapFunction[String,String] {

      override def open(parameters: Configuration): Unit = {
        val file = getRuntimeContext.getDistributedCache.getFile("ds-file")
        val list = FileUtils.readLines(file)
        /*for (i <- 0 until  list.size()) {
          println(list.get(i))
        }*/

        import scala.collection.JavaConverters._
        for (ele <- list.asScala){
          println(ele)
        }
      }

      override def map(value: String): String = {
        value
      }
    }).print()

  }
}
