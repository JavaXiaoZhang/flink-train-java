package com.flink.project

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Requests, RestClientBuilder}
import org.slf4j.LoggerFactory

object LogAnalysis {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val topic = "zzqq"

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.33.10:9092")
    properties.setProperty("group.id", "zq-group")

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, properties)
    val data = env.addSource(consumer)
    //data.print().setParallelism(1)

    val logData = data.map(x => {
      val splits = x.split("\t")
      val level = splits(2)
      val timeStr = splits(3)
      var time = 0l
      try {
        //DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").
        time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeStr).getTime
      } catch {
        case e: Exception => {
          logger.error(s"time parse error: $timeStr", e.getMessage)
        }
      }
      //val ip = splits(4)
      val domain = splits(5)
      val traffic = splits(6).toLong
      (level, time, domain, traffic)
    }).filter(_._2 != 0).filter(_._1 == "E")
      .map(x => {
        // 抛弃_1
        (x._2, x._3, x._4)
      })

    val resultData = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {

      val maxOutOfOrderness = 10000L

      var currentMaxTimestamp: Long = _

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1).window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .apply(new WindowFunction[(Long, String, Long), (String, String, Long), Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {
          val domain = key.getField(0).toString
          var sum = 0l
          var timeStr = ""
          val iterator = input.iterator
          while (iterator.hasNext) {
            val next = iterator.next()
            sum += next._3 // traffic求和

            val time = next._1
            val date = new Date(time)
            timeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
          }

          /**
           * 1，这一分钟的时间 2019-09-09 10:10
           * 2，域名
           * 3，traffic的和
           */
          out.collect((timeStr, domain, sum))
        }
      })

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[String](
      httpHosts,
      new ElasticsearchSinkFunction[String] {
        def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = new java.util.HashMap[String, String]
          json.put("data", element)

          val rqst: IndexRequest = Requests.indexRequest
            .index("my-index")
            .`type`("my-type")
            .source(json)

          indexer.add(rqst)
        }
      }
    )

    // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
    esSinkBuilder.setBulkFlushMaxActions(1)

    // provide a RestClientFactory for custom configuration on the internally created REST client
    esSinkBuilder.setRestClientFactory(new RestClientFactory {
      override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
        restClientBuilder.setDefaultHeaders(Array())
        restClientBuilder.setMaxRetryTimeoutMillis(null)
        restClientBuilder.setPathPrefix("")
        restClientBuilder.setHttpClientConfigCallback(null)
      }
    })

    // finally, build and add the sink to the job's pipeline
    resultData.addSink(esSinkBuilder.build)


    env.execute("LogAnalysis")
  }
}
