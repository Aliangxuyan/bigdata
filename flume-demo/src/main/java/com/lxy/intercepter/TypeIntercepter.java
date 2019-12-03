package com.lxy.intercepter;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * bin/flume-ng agent --conf conf/ --name a3 --conf-file job/interceptor/flume3.conf - Dflume.root.logger=INFO,console
 *
 * bin/flume-ng agent --conf conf/ --name a4 --conf-file job/interceptor/flume4.conf - Dflume.root.logger=INFO,console
 *
 * bin/flume-ng agent --conf conf/ --name a1 --conf-file job/interceptor/flume2.conf
 *
 * @author lxy
 * @date 2019-12-02
 */
public class TypeIntercepter implements Interceptor {

    // 申明一个存放事件的集合
    private List<Event> addHeaderEvents;

    public void initialize() {
        //初始化
        addHeaderEvents = new ArrayList<Event>();
    }

    /**
     * 单个事件拦截
     *
     * @param event
     * @return
     */
    public Event intercept(Event event) {

        // 1、获取事件中的头信息
        Map<String, String> headers = event.getHeaders();

        // 2、获取事件中的body 信息
        String body = new String(event.getBody());

        // 3、根据body 中是否有"hello 来决定添加怎样的头信息"
        if (body.contains("hello")) {
            //4、添加头信息
            headers.put("type", "atguigu");
        } else {
            headers.put("type", "bigdata");
        }
        event.setHeaders(headers);
        return event;
    }


    /**
     * 批量事件拦截
     *
     * @param events
     * @return
     */
    public List<Event> intercept(List<Event> events) {
        // 1、清空集合
        addHeaderEvents.clear();

        //2、遍历events
        for (Event event : events) {
            //3、给每一个事件添加头信息
            addHeaderEvents.add(intercept(event));
        }
        // 4、返回结果
        return addHeaderEvents;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new TypeIntercepter();
        }

        public void configure(Context context) {

        }
    }
}
