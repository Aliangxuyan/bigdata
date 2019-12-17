package com.lxy.common.bean;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author lxy
 * @date 2019-11-18
 * 生产者接口
 */
public interface Producer extends Closeable {

    public void setIn(DataIN in);

    public void setOut(DataOut out);

    /**
     * c生产数据
     */
    public void producer();

    /**
     * 关闭资源
     */
    public void close() throws IOException;
}
