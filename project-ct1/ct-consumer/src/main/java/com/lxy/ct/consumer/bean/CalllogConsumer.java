package com.lxy.ct.consumer.bean;

import com.lxy.common.bean.Consumer;
import com.lxy.common.constant.Names;
import com.lxy.ct.consumer.dao.HbaseDao;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * 通话日志的消费
 *
 * @author lxy
 * @date 2019-12-15
 */
public class CalllogConsumer implements Consumer {
    /**
     * 消费消息
     */
    public void consumer() {
        try {
            Properties prop = new Properties();
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));

            // 获取flume  采集的数据
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

            //关注主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));

            // Hbase 数据访问对象
            HbaseDao dao = new HbaseDao();
            // 初始化
            dao.init();

            // 消费数据
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println("接受的kafka  数据" + consumerRecord.value());
                    if (StringUtils.isNotEmpty(consumerRecord.value())) {
//                    Calllog log = new Calllog(consumerRecord.value());
//                    dao.insertData(log);
                        dao.inserData(consumerRecord.value());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭资源
     *
     * @throws IOException
     */
    public void close() throws IOException {

    }
}
