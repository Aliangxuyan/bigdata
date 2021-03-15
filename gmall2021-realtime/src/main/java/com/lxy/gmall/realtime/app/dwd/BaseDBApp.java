package com.lxy.gmall.realtime.app.dwd;

import com.lxy.gmall.realtime.utils.MyKafkaUtil;

/**
 * @author lxy
 * @date 2021/3/15
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        //Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //设置CK相关参数
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 1.接收Kafka数据，过滤空值数据
        //定义消费者组以及指定消费主题
        String topic = "ods_base_db_m";
        String groupId = "ods_base_group";

        //从Kafka主题中读取数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStream<String> jsonDstream = env.addSource(kafkaSource);
        //jsonDstream.print("data json:::::::");

        //对数据进行结构的转换   String->JSONObject
        DataStream<JSONObject> jsonStream = jsonDstream.map(jsonStr -> JSON.parseObject(jsonStr));
        //DataStream<JSONObject>  jsonStream   = jsonDstream.map(JSON::parseObject);

        //过滤为空或者 长度不足的数据
        SingleOutputStreamOperator<JSONObject> filteredDstream = jsonStream.filter(
            jsonObject -> {
                boolean flag = jsonObject.getString("table") != null
                    && jsonObject.getJSONObject("data") != null
                    && jsonObject.getString("data").length() > 3;
                return flag;
            });
        filteredDstream.print("json::::::::");

        env.execute();
    }
}
}
