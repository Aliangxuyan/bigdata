package com.lxy.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxy.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author lxy
 * @date 2021/3/17
 * 访客跳出情况判断
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //  注意：flink1.12默认的时间语义就是事件时间，所以不需要执行
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //TODO 1.从kafka的dwd_page_log主题中读取页面日志
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";

        //从kafka中读取数据
        DataStreamSource<String> dataStream = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

//        // 利用测试数据验证
//        DataStream<String> dataStream = env
//            .fromElements(
//                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                    "\"home\"},\"ts\":15000} ",
//                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                    "\"detail\"},\"ts\":30000} "
//            );
//        dataStream.print("in json:");


        //对数据进行结构的转换
        DataStream<JSONObject> jsonObjStream = dataStream.map(jsonString -> JSON.parseObject(jsonString));

        jsonObjStream.print("json:");

        //TODO 2.指定事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithEtDstream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long recordTimestamp) {
                return jsonObject.getLong("ts");
            }
        }));
        //TODO 3.根据日志数据的mid进行分组
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonObjWithEtDstream.keyBy(
            jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );
        //TODO 4.配置CEP表达式
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("GoIn").where(
            new SimpleCondition<JSONObject>() {
                @Override   // 条件1 ：进入的第一个页面
                public boolean filter(JSONObject jsonObj) throws Exception {
                    String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                    System.out.println("first in :" + lastPageId);
                    if (lastPageId == null || lastPageId.length() == 0) {
                        return true;
                    }
                    return false;
                }
            }
        ).next("next").where(
            new SimpleCondition<JSONObject>() {
                @Override  //条件2： 在10秒时间范围内必须有第二个页面
                public boolean filter(JSONObject jsonObj) throws Exception {
                    String pageId = jsonObj.getJSONObject("page").getString("page_id");
                    System.out.println("next:" + pageId);
                    if (pageId != null && pageId.length() > 0) {
                        return true;
                    }
                    return false;
                }
            }
        ).within(Time.milliseconds(10000));

        //TODO 5.根据表达式筛选流
        PatternStream<JSONObject> patternedStream = CEP.pattern(jsonObjectStringKeyedStream, pattern);

        //TODO 6. 提取命中的数据
        final OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };
        SingleOutputStreamOperator<String> filteredStream = patternedStream.flatSelect(
            timeoutTag,
            new PatternFlatTimeoutFunction<JSONObject, String>() {
                @Override
                public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                    List<JSONObject> objectList = pattern.get("GoIn");
                    //这里进入out的数据都被timeoutTag标记
                    for (JSONObject jsonObject : objectList) {
                        out.collect(jsonObject.toJSONString());
                    }
                }
            },
            new PatternFlatSelectFunction<JSONObject, String>() {
                @Override
                public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                    //因为不超时的事件不提取，所以这里不写代码
                }
            });
        //通过SideOutput侧输出流输出超时数据
        DataStream<String> jumpDstream = filteredStream.getSideOutput(timeoutTag);
        jumpDstream.print("jump::");

        //TODO 7.将跳出数据写回到kafka的DWM层
        jumpDstream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
