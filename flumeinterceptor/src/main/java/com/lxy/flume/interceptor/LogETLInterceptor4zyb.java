package com.lxy.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogETLInterceptor4zyb implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        // etl
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        if (StringUtils.isNotEmpty(log)){
            String[] split = log.split("\t");
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("channel_id", split[0]);
            jsonObject.put("activity_id", split[1]);
            jsonObject.put("unionid", split[2]);
            jsonObject.put("event_id", split[3]);
            jsonObject.put("event_time", split[4]);
            jsonObject.put("create_time", split[5]);
            jsonObject.put("act_type", split[6]);
            jsonObject.put("wxid", "");
            jsonObject.put("wx_group_id", "");
            System.out.println("****" + jsonObject.toJSONString());
            // 2 获取header
            Map<String, String> headers = event.getHeaders();
            headers.put("topic","llxxyy");
            event.setBody(jsonObject.toJSONString().getBytes());
            return event;
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : events) {
            Event intercept1 = intercept(event);

            if (intercept1 != null){
                interceptors.add(event);
            }
        }

        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor4zyb();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
