package com.lxy.common.bean;

/**
 * @author lxy
 * @date 2019-11-18
 * 生产者接口
 */
public interface Producer {

    public void setIn(DataIN in);
    public void setOut(DataOut out);

    /**
     * c生产数据
     */
    public void producer();

    /**
     * 关闭资源
     */
    public void close();
}
