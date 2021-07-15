package base.chapter05;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO

 */
public class Flink01_Environment {
    public static void main(String[] args) {
        // 获取 批处理 执行环境
        ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();
        // 获取 流处理 执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
