package com.flink.sink;

import com.flink.dataset.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomSinkToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Student> studentStream = source.map((MapFunction<String, Student>) value -> {
            String[] splits = value.split(",");
            return new Student(splits[0], Integer.valueOf(splits[1]), splits[2]);
        });
        studentStream.addSink(new SinkToMysql());

        env.execute("aaa");
    }
}
