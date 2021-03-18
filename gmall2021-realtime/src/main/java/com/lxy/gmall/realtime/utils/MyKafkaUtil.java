package com.lxy.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author lxy
 * @date 2021/3/15
 */
public class MyKafkaUtil {
    private static String kafkaServer = "hadoop202:9092,hadoop203:9092,hadoop204:9092";
    private static final String DEFAULT_TOPIC = "DEFAULT_DATA";

    //封装Kafka消费者
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
    }

    //封装Kafka生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<>(kafkaServer, topic, new SimpleStringSchema());
    }

    //封装Kafka生产者  动态指定多个不同主题
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> serializationSchema) {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        //如果15分钟没有更新状态，则超时 默认1分钟
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 60 * 15 + "");
        return new FlinkKafkaProducer<>(DEFAULT_TOPIC, serializationSchema, prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka', " +
            " 'topic' = '"+topic+"',"   +
            " 'properties.bootstrap.servers' = '"+ kafkaServer +"', " +
            " 'properties.group.id' = '"+groupId+ "', " +
            "  'format' = 'json', " +
            "  'scan.startup.mode' = 'latest-offset'  ";
        return  ddl;
    }
}
