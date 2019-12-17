package com.lxy.common.bean;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author lxy
 * @date 2019-12-15
 */
public interface Consumer extends Closeable {
    /**
     * 消费接口
     */
    public void consumer();

    /**
     * 关闭资源
     */
    public void close() throws IOException;
}
