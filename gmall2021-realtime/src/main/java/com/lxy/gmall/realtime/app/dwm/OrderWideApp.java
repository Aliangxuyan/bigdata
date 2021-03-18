package com.lxy.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxy.gmall.realtime.app.func.DimAsyncFunction;
import com.lxy.gmall.realtime.bean.OrderDetail;
import com.lxy.gmall.realtime.bean.OrderInfo;
import com.lxy.gmall.realtime.bean.OrderWide;
import com.lxy.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

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

        //TODO 5.关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDstream = AsyncDataStream.unorderedWait(
            orderWideDstream, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                @Override
                public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                    SimpleDateFormat formattor = new SimpleDateFormat("yyyy-MM-dd");
                    String birthday = jsonObject.getString("BIRTHDAY");
                    Date date = formattor.parse(birthday);

                    Long curTs = System.currentTimeMillis();
                    Long betweenMs = curTs - date.getTime();
                    Long ageLong = betweenMs / 1000L / 60L / 60L / 24L / 365L;
                    Integer age = ageLong.intValue();
                    orderWide.setUser_age(age);
                    orderWide.setUser_gender(jsonObject.getString("GENDER"));
                }

                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getUser_id());
                }
            }, 60, TimeUnit.SECONDS);

        orderWideWithUserDstream.print("dim join user:");

        //TODO 6.关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDstream = AsyncDataStream.unorderedWait(
            orderWideWithUserDstream, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                @Override
                public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                    orderWide.setProvince_name(jsonObject.getString("NAME"));
                    orderWide.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"));
                    orderWide.setProvince_iso_code(jsonObject.getString("ISO_CODE"));
                    orderWide.setProvince_area_code(jsonObject.getString("AREA_CODE"));
                }

                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getProvince_id());
                }
            }, 60, TimeUnit.SECONDS);

        //TODO 7.关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDstream = AsyncDataStream.unorderedWait(
            orderWideWithProvinceDstream, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                @Override
                public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                    orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                    orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                    orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                    orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                }

                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getSku_id());
                }
            }, 60, TimeUnit.SECONDS);

        //TODO 8.关联SPU商品维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDstream = AsyncDataStream.unorderedWait(
            orderWideWithSkuDstream, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                @Override
                public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                    orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                }

                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getSpu_id());
                }
            }, 60, TimeUnit.SECONDS);

        //TODO 9.关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3Dstream = AsyncDataStream.unorderedWait(
            orderWideWithSpuDstream, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                @Override
                public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                    orderWide.setCategory3_name(jsonObject.getString("NAME"));
                }

                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getCategory3_id());
                }
            }, 60, TimeUnit.SECONDS);

        //TODO 10.关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDstream = AsyncDataStream.unorderedWait(
            orderWideWithCategory3Dstream, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                @Override
                public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                    orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                }

                @Override
                public String getKey(OrderWide orderWide) {
                    return String.valueOf(orderWide.getTm_id());
                }
            }, 60, TimeUnit.SECONDS);

        //TODO 11.将订单和订单明细Join之后以及维度关联的宽表写到Kafka的dwm层
        orderWideWithTmDstream.map(orderWide -> JSON.toJSONString(orderWide))
            .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        env.execute();
    }
}
