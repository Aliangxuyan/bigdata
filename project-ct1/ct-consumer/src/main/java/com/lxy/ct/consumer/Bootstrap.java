package com.lxy.ct.consumer;

import com.lxy.common.bean.Consumer;
import com.lxy.ct.consumer.bean.CalllogConsumer;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-12-15
 * 启动消费者，使用kafka 消费者获取flume  采集的数据
 * 将数据存储到Hbase  中去
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
        //创建消费者
        Consumer consumer = new CalllogConsumer();

        //使用kafka 消费者获取flume 采集的数据
        consumer.consumer();

        // 将数据存储到hbase 中去

        // 关闭资源
        consumer.close();
    }
}
