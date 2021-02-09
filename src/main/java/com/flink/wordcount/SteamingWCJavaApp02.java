package com.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 流处理
 */
public class SteamingWCJavaApp02 {

    /**
     * keyBy和sum 通过Tuple传参方式指定index
     * @param args
     * @throws Exception
     */
    public static void main2(String[] args) throws Exception {

        Integer port = 0;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            port = 9999;
            System.err.println("端口未设置，使用默认端口9999");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9999);
        textStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = line.toLowerCase().split(",");
                for (String s : split) {
                    collector.collect(new Tuple2<>(s,1));
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1);
        env.execute("SteamingWCJavaApp");

    }

    /**
     * keyBy和sum 通过自定义类传参方式指定field名称
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Integer port = 0;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            port = 9999;
            System.err.println("端口未设置，使用默认端口9999");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textStream = env.socketTextStream("localhost", port);
        textStream.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String line, Collector<WordCount> collector) throws Exception {
                String[] split = line.toLowerCase().split(",");
                for (String s : split) {
                    collector.collect(new WordCount(s,1));
                }
            }
        })
                //.keyBy("word")
                .keyBy(new KeySelector<WordCount, String>() { // keySelector方式
                    @Override
                    public String getKey(WordCount wordCount) throws Exception {
                        return wordCount.word;
                    }
                })
                .timeWindow(Time.seconds(5)).sum("count").print().setParallelism(1);
        env.execute("SteamingWCJavaApp");

    }

    public static class WordCount{
        private String word;
        private Integer count;

        public WordCount() {
        }

        public WordCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
