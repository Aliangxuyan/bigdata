package com.lxy.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author lxy
 * @date 2021/3/18
 * 维度关联查询的接口
 */
public interface DimJoinFunction<T> {
    /**
     * 需要实现如何把结果装配给数据流对象
     *
     * @param t          数据流对象
     * @param jsonObject 异步查询结果
     * @throws Exception
     */
    public void join(T t, JSONObject jsonObject) throws Exception;

    /**
     * 需要实现如何从流中对象获取主键
     *
     * @param t 数据流对象
     */
    public String getKey(T t);
}
