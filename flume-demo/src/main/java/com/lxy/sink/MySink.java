package com.lxy.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lxy
 * @date 2019-12-03
 */
public class MySink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(MySink.class);
    // 定义两个属性
    private String prefix;
    private String suffix;

    public void configure(Context context) {
        // 读取配置信息给其赋值
        //读取配置文件内容，无默认值
        prefix = context.getString("prefix");
        //读取配置文件内容，有默认值
        suffix = context.getString("suffix", "atguigu");
    }

    /**
     * 1、获取channel ，
     * 2、从channel获取事务已经数据
     * 3、发送数据
     *
     * @return
     * @throws EventDeliveryException
     */
    public Status process() throws EventDeliveryException {
        Status status = null;

        // 1、获取channel
        Channel channel = getChannel();

        //2、从channel获取事务已经数据
        Transaction transaction = channel.getTransaction();

        //3、开启事务
        transaction.begin();

        try {
            //4、从channel中获取数据
//            Event event = channel.take();
            Event event;
            //读取 Channel 中的事件，直到读取到事件结束循环
            while (true) {
                event = channel.take();
                if (event != null) {
                    break;
                }
            }

            //5、处理事件(打印)
            if (event != null) {
                String bodyStr = new String(event.getBody());
                logger.info(prefix + bodyStr + suffix);
            }

            //6、提交事务
            transaction.commit();
            //7、提交成功，修改状态信息
            status = Status.READY;
        } catch (ChannelException e) {
            e.printStackTrace();
            //8、提交事务失败
            transaction.rollback();
            //9、修改事务
            status = Status.BACKOFF;

        } finally {
            //10、最终关闭事务
            if (transaction != null) {
                transaction.close();
            }
        }
        return status;
    }


}
