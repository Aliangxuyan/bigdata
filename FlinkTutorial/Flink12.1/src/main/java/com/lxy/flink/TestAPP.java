package com.lxy.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lxy
 * @date 2021/7/15
 */
public class TestAPP {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeMode.BATCH);
    }
}
