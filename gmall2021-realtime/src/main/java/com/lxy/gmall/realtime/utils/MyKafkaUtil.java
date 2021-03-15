package com.lxy.gmall.realtime.utils;

/**
 * @author lxy
 * @date 2021/3/15
 */
public class MyKafkaUtil {
    private static String kafkaServer = "hadoop202:9092,hadoop203:9092,hadoop204:9092";

    //封装Kafka消费者
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
    }
    //封装Kafka生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<>(kafkaServer,topic,new SimpleStringSchema());
    }
}
