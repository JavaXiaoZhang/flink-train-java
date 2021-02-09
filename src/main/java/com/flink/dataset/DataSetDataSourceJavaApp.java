package com.flink.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class DataSetDataSourceJavaApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //fromCollection(env);
        readTextFile(env);
    }

    private static void readTextFile(ExecutionEnvironment env) throws Exception {
        env.readTextFile("./data/data.txt").print();
    }


    private static void fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }

        env.fromCollection(list).print();
    }
}
