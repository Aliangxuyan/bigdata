package com.lxy.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.lxy.gmall.realtime.bean.OrderWide;
import com.lxy.gmall.realtime.bean.PaymentInfo;
import com.lxy.gmall.realtime.bean.PaymentWide;
import com.lxy.gmall.realtime.utils.DateTimeUtil;
import com.lxy.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author lxy
 * @date 2021/3/18
 * 支付宽表处理主程序
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
         /*
        //设置CK相关配置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend("hdfs://hadoop:8020/gmall/flink/checkpoint/OrderWideApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        */

        //TODO 1.接收数据流
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        //封装Kafka消费者  读取支付流数据
        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);
        DataStream<String> paymentInfojsonDstream = env.addSource(paymentInfoSource);
        //对读取的支付数据进行转换
        DataStream<PaymentInfo> paymentInfoDStream =
            paymentInfojsonDstream.map(jsonString -> JSON.parseObject(jsonString, PaymentInfo.class));

        //封装Kafka消费者  读取订单宽表流数据
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStream<String> orderWidejsonDstream = env.addSource(orderWideSource);
        //对读取的订单宽表数据进行转换
        DataStream<OrderWide> orderWideDstream =
            orderWidejsonDstream.map(jsonString -> JSON.parseObject(jsonString, OrderWide.class));

        //设置水位线
        SingleOutputStreamOperator<PaymentInfo> paymentInfoEventTimeDstream =
            paymentInfoDStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner(
                        (paymentInfo, ts) -> DateTimeUtil.toTs(paymentInfo.getCallback_time())
                    ));

        SingleOutputStreamOperator<OrderWide> orderInfoWithEventTimeDstream =
            orderWideDstream.assignTimestampsAndWatermarks(WatermarkStrategy.
                <OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    (orderWide, ts) -> DateTimeUtil.toTs(orderWide.getCreate_time())
                )
            );

        //设置分区键
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedStream =
            paymentInfoEventTimeDstream.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideKeyedStream =
            orderInfoWithEventTimeDstream.keyBy(OrderWide::getOrder_id);

        //关联数据
        SingleOutputStreamOperator<PaymentWide> paymentWideDstream =
            paymentInfoKeyedStream.intervalJoin(orderWideKeyedStream).
                between(Time.seconds(-1800), Time.seconds(0)).
                process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo,
                                               OrderWide orderWide,
                                               Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                }).uid("payment_wide_join");

        SingleOutputStreamOperator<String> paymentWideStringDstream = paymentWideDstream.map(paymentWide -> JSON.toJSONString(paymentWide));
        paymentWideStringDstream.print("pay:");
        paymentWideStringDstream.addSink(
            MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        env.execute();

    }
}
