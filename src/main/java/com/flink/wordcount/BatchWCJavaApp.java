package com.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 使用Java API开发flink批处理应用程序
 */
public class BatchWCJavaApp {
    public static void main(String[] args) throws Exception {
        String dataPath = "./data/data.txt";
        // 1. 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 读数据
        DataSource<String> textFile = env.readTextFile(dataPath);
        // 3. transform
        textFile.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, collector) -> {
            String[] split = line.toLowerCase().split(" ");
            for (String s : split) {
                collector.collect(new Tuple2<>(s, 1));
            }
        }).groupBy(0).sum(1).print();
    }
}
