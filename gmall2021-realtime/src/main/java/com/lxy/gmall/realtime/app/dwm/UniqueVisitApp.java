package com.lxy.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxy.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * @author lxy
 * @date 2021/3/17
 * 访客UV的计算
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
//TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //StateBackend fsStateBackend = new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint/UniqueVisitApp");
        //env.setStateBackend(fsStateBackend);
        //System.setProperty("HADOOP_USER_NAME", "atguigu");
        //TODO 1.从Kafka中读取数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        //读取kafka数据
        FlinkKafkaConsumer<String> source = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStream = env.addSource(source);

        //对读取的数据进行结构的转换
        DataStream<JSONObject> jsonObjStream = jsonStream.map(jsonString -> JSON.parseObject(jsonString));

        jsonObjStream.print("uv:");

        //TODO 2.核心的过滤代码
        //按照设备id进行分组
        KeyedStream<JSONObject, String> keyByWithMidDstream =
            jsonObjStream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filteredJsonObjDstream =
            keyByWithMidDstream.filter(new RichFilterFunction<JSONObject>() {
                //定义状态用于存放最后访问的日期
                ValueState<String> lastVisitDateState = null;
                //日期格式
                SimpleDateFormat simpleDateFormat = null;

                //初始化状态 以及时间格式器
                @Override
                public void open(Configuration parameters) throws Exception {
                    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    if (lastVisitDateState == null) {
                        ValueStateDescriptor<String> lastViewDateStateDescriptor =
                            new ValueStateDescriptor<>("lastViewDateState", String.class);
                        //因为统计的是当日UV，也就是日活，所有为状态设置失效时间
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                        //默认值 表明当状态创建或每次写入时都会更新时间戳
                        //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        //默认值  一旦这个状态过期了，那么永远不会被返回给调用方，只会返回空状态
                        //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();
                        lastViewDateStateDescriptor.enableTimeToLive(stateTtlConfig);
                        lastVisitDateState = getRuntimeContext().getState(lastViewDateStateDescriptor);
                    }
                }

                //首先检查当前页面是否有上页标识，如果有说明该次访问一定不是当日首次
                @Override
                public boolean filter(JSONObject jsonObject) throws Exception {
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId != null && lastPageId.length() > 0) {
                        return false;
                    }
                    Long ts = jsonObject.getLong("ts");
                    String logDate = simpleDateFormat.format(ts);
                    String lastViewDate = lastVisitDateState.value();

                    if (lastViewDate != null && lastViewDate.length() > 0 && logDate.equals(lastViewDate)) {
                        System.out.println("已访问：lastVisit:" + lastViewDate + "|| logDate：" + logDate);
                        return false;
                    } else {
                        System.out.println("未访问：lastVisit:" + lastViewDate + "|| logDate：" + logDate);
                        lastVisitDateState.update(logDate);
                        return true;
                    }
                }
            }).uid("uvFilter");

        SingleOutputStreamOperator<String> dataJsonStringDstream =
            filteredJsonObjDstream.map(jsonObj -> jsonObj.toJSONString());
        dataJsonStringDstream.print("uv");
        //        2.2.3将过滤处理后的UV写入到Kafka的dwm_unique_visit
        //TODO 3.将过滤处理后的UV写入到Kafka的dwm主题
        dataJsonStringDstream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
