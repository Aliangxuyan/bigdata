package com.lxy.producer;

import com.lxy.common.bean.Producer;
import com.lxy.producer.bean.LocalFileProducer;
import com.lxy.producer.io.LocalFileDateIn;
import com.lxy.producer.io.LocalFileDateOut;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-11-18
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("系统参数不正确，请按照制定格式传递 java ");
        }
        // 构造生产者对象
        Producer producer = new LocalFileProducer();

        producer.setIn(new LocalFileDateIn(args[0]));
        producer.setOut(new LocalFileDateOut(args[1]));

        // 生产数据
        producer.producer();

        // 关闭生产者对象
        producer.close();
    }

}
