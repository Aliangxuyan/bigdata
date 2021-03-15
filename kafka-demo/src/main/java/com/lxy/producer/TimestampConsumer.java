package com.lxy.producer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author lxy
 * @date 2020/12/21
 *
 * 重置 kafka consumer offset 点，todo ??? 有部分go 和php 创建的offset点获取不到信息 python API 存在同样问题
 */
public class TimestampConsumer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.130.111:9092,192.168.130.115:9092,192.168.130.104:9092");
        props.put("group.id", "my_group_new");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        String topic = "topic_yk_laxindata_rwb_activity";

        try {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();

            Map<TopicPartition, Long> timestampsToSearch = new HashMap<TopicPartition, Long>();
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date now = new Date();
            long nowTime = now.getTime();
            System.out.println("当前时间：" + df.format(now));
            long fetchDataTime = nowTime - 1000 * 60 * 30 * 10; // 计算30分钟之前的时间戳
            fetchDataTime = 1608440400000l; // 计算30分钟之前的时间戳
//            long fetchDataTime = 0; // 计算30分钟之前的时间戳

            for (PartitionInfo partitionInfo : partitionInfos) {
                topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                timestampsToSearch.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), fetchDataTime);
            }
            consumer.assign(topicPartitions);

            //  获取每个partition一个小时之前的偏移量
            Map<TopicPartition, OffsetAndTimestamp> map = consumer.offsetsForTimes(timestampsToSearch);
            System.out.println("map:" + map.size());

            OffsetAndTimestamp offsetTimestamp = null;
            System.out.println("开始设置各分区初始偏移量...");
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : map.entrySet()) {
                System.out.println("********************************************************************");
                System.out.println("entry:{}" + entry);
                // 如果设置的查询偏移量的时间点大于最大的索引记录时间，那么value就为空
                offsetTimestamp = entry.getValue();
                if (offsetTimestamp != null) {
                    int partition = entry.getKey().partition();
                    long timestamp = offsetTimestamp.timestamp();
                    long offset = offsetTimestamp.offset();
                    System.out.println("partition = " + partition +
                        ", time = " + df.format(new Date(timestamp)) +
                        ", offset = " + offset);
                    // 设置读取消息的偏移量
                    consumer.seek(entry.getKey(), offset);
                }
            }
            System.out.println("设置各分区初始偏移量结束...");

//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(1000);
//                for (ConsumerRecord<String, String> record : records) {
//
//                    System.out.println("partition = " + record.partition() + ", offset = " + record.offset() + "，record:" + record.toString());
//                }
//            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }
}
