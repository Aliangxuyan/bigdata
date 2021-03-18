package com.lxy.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.lxy.gmall.realtime.utils.DimUtil;
import com.lxy.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

/**
 * @author lxy
 * @date 2021/3/18
 * <p>
 * 自定义维度查询异步执行函数
 * * RichAsyncFunction：   里面的方法负责异步查询
 * * DimJoinFunction：     里面的方法负责将为表和主流进行关联
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    ExecutorService executorService = null;

    public String tableName = null;


    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    public void open(Configuration parameters) {
        System.out.println("获得线程池！ ");
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    long start = System.currentTimeMillis();
                    //从流对象中获取主键
                    String key = getKey(obj);
                    //根据主键获取维度对象数据
                    JSONObject dimJsonObject = DimUtil.getDimInfo(tableName, key);
                    System.out.println("dimJsonObject:" + dimJsonObject);
                    if (dimJsonObject != null) {
                        //维度数据和流数据关联
                        join(obj, dimJsonObject);
                    }
                    System.out.println("obj:" + obj);
                    long end = System.currentTimeMillis();
                    System.out.println("异步耗时：" + (end - start) + "毫秒");
                    resultFuture.complete(Arrays.asList(obj));
                } catch (Exception e) {
                    System.out.println(String.format(tableName + "异步查询异常. %s", e));
                    e.printStackTrace();
                }
            }
        });
    }
}
