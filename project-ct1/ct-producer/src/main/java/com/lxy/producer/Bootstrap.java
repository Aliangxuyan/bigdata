package com.lxy.producer;

import com.lxy.common.bean.Producer;
import com.lxy.producer.bean.LocalFileProducer;

/**
 * @author lxy
 * @date 2019-11-18
 * 启动对象
 * */
public class Bootstrap {
    public static void main(String[] args) {
        // 构造生产者对象
        Producer producer = new LocalFileProducer();

        // 生产数据
        producer.producer();

        // 关闭生产者对象
        producer.close();
    }

}
