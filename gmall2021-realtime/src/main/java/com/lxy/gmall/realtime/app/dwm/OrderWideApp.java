package com.lxy.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.lxy.gmall.realtime.bean.OrderDetail;
import com.lxy.gmall.realtime.bean.OrderInfo;
import com.lxy.gmall.realtime.bean.OrderWide;
import com.lxy.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @author lxy
 * @date 2021/3/17
 * 处理订单和订单明细数据形成订单宽表
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度读取kafka分区数据
        env.setParallelism(4);
        /*
        //设置CK相关配置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend("hdfs://hadoop:8020/gmall/flink/checkpoint/OrderWideApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        */
        //TODO 1.从Kafka的dwd层接收订单和订单明细数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        //从Kafka中读取数据
        FlinkKafkaConsumer<String> sourceOrderInfo = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> sourceOrderDetail = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStream<String> orderInfojsonDStream = env.addSource(sourceOrderInfo);
        DataStream<String> orderDetailJsonDStream = env.addSource(sourceOrderDetail);


        //对读取的数据进行结构的转换
        DataStream<OrderInfo> orderInfoDStream = orderInfojsonDStream.map(
            new RichMapFunction<String, OrderInfo>() {
                SimpleDateFormat simpleDateFormat = null;

                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                }

                @Override
                public OrderInfo map(String jsonString) throws Exception {
                    OrderInfo orderInfo = JSON.parseObject(jsonString, OrderInfo.class);
                    orderInfo.setCreate_ts(simpleDateFormat.parse(orderInfo.getCreate_time()).getTime());
                    return orderInfo;
                }
            }
        );
        DataStream<OrderDetail> orderDetailDStream = orderDetailJsonDStream.map(new RichMapFunction<String, OrderDetail>() {
            SimpleDateFormat simpleDateFormat = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public OrderDetail map(String jsonString) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(jsonString, OrderDetail.class);
                orderDetail.setCreate_ts(simpleDateFormat.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });

        orderInfoDStream.print("orderInfo::::");
        orderDetailDStream.print("orderDetail::::");

        //TODO 2.设定事件时间水位
        SingleOutputStreamOperator<OrderInfo> orderInfoWithEventTimeDstream = orderInfoDStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                @Override
                public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                    return orderInfo.getCreate_ts();
                }
            })
        );

        SingleOutputStreamOperator<OrderDetail> orderDetailWithEventTimeDstream = orderDetailDStream.assignTimestampsAndWatermarks(
            WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {

                @Override
                public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                    return orderDetail.getCreate_ts();
                }
            }));

        //TODO 3.设定关联的key
        KeyedStream<OrderInfo, Long> orderInfoKeyedDstream = orderInfoWithEventTimeDstream.keyBy(orderInfo -> orderInfo.getId());
        KeyedStream<OrderDetail, Long> orderDetailKeyedStream = orderDetailWithEventTimeDstream.keyBy(orderDetail -> orderDetail.getOrder_id());

        //TODO 4.订单和订单明细关联 intervalJoin
        SingleOutputStreamOperator<OrderWide> orderWideDstream = orderInfoKeyedDstream.intervalJoin(orderDetailKeyedStream)
            .between(Time.seconds(-5), Time.seconds(5))
            .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                @Override
                public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                    out.collect(new OrderWide(orderInfo, orderDetail));
                }
            });

        orderWideDstream.print("joined ::");

        env.execute();
    }
}
