package com.lxy.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxy.gmall.realtime.app.func.DimSink;
import com.lxy.gmall.realtime.app.func.TableProcessFunction;
import com.lxy.gmall.realtime.bean.TableProcess;
import com.lxy.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author lxy
 * @date 2021/3/15
 * 从Kafka中读取ods层业务数据 并进行处理  发送到DWD层
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

        //TODO 2.动态分流  事实表放入主流，作为DWD层；维度表放入侧输出流
//定义输出到Hbase的侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };

//使用自定义ProcessFunction进行分流处理
        SingleOutputStreamOperator<JSONObject> kafkaDStream = filteredDstream.process(new TableProcessFunction(hbaseTag));

//获取侧输出流，即将通过Phoenix写到Hbase的数据
        DataStream<JSONObject> hbaseDStream = kafkaDStream.getSideOutput(hbaseTag);

        //TODO 3.将侧输出流数据写入HBase(Phoenix)
        hbaseDStream.print("hbase::::");
        hbaseDStream.addSink(new DimSink());


        //TODO 4将主流数据写入Kafka
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("启动Kafka Sink");
            }

            //从每条数据得到该条数据应送往的主题名
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                String topic = jsonObject.getString("sink_table");
                JSONObject dataJsonObj = jsonObject.getJSONObject("data");
                return new ProducerRecord(topic, dataJsonObj.toJSONString().getBytes());
            }
        });
        kafkaDStream.print("kafka ::::");
        kafkaDStream.addSink(kafkaSink);

        env.execute();
    }
}
