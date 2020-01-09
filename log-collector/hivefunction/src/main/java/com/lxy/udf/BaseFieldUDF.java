package com.lxy.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author lxy
 * @date 2019-12-29
 */
public class BaseFieldUDF extends UDF {

    public static String evaluate(String line, String jsonkeysString) {
        StringBuilder sb = new StringBuilder();

        //1、获取所有key, mid uv......
        String[] jsonKeys = jsonkeysString.split(",");

        //2、line 服务器时间|json 
        String[] logContents = line.split("\\|");

        //3、校验
        if (logContents.length != 2 || StringUtils.isBlank(logContents[1])) {
            return "";
        }

        try {
            //4、对logContents【1】创建json 对象
            JSONObject jsonObject = new JSONObject(logContents[1]);

            // 5、获取公共字端的json
            JSONObject cmJson = jsonObject.getJSONObject("cm");

            //6、循环遍历
            for (int i = 0; i < jsonKeys.length; i++) {
                String jsonKey = jsonKeys[i].trim();
                if (cmJson.has(jsonKey)) {
                    sb.append(cmJson.getString(jsonKey)).append("\t");
                } else {
                    sb.append("\t");
                }
            }

            // 7，拼接时间字端和服务器时间处理
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(logContents[0]).append("\t");

        } catch (JSONException e) {
            e.printStackTrace();
        }

        return sb.toString();

    }

    /**
     * 数据测试
     *
     * @param args
     */
    public static void main(String[] args) {
        String line = "1577245615967|{\"cm\":{\"ln\":\"-42.8\",\"sv\":\"V2.3.3\",\"os\":\"8.2.8\",\"g\":\"D72GM4R8@gmail.com\",\"mid\":\"45\",\"nw\":\"4G\",\"l\":\"en\",\"vc\":\"1\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"45\",\"t\":\"1577223586139\",\"la\":\"5.8\",\"md\":\"HTC-5\",\"vn\":\"1.0.3\",\"ba\":\"HTC\",\"sr\":\"E\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1577169787243\",\"en\":\"display\",\"kv\":{\"goodsid\":\"15\",\"action\":\"1\",\"extend1\":\"2\",\"place\":\"5\",\"category\":\"93\"}},{\"ett\":\"1577186956633\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"48\",\"action\":\"2\",\"extend1\":\"\",\"type\":\"1\",\"type1\":\"201\",\"loading_way\":\"2\"}},{\"ett\":\"1577161889060\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}},{\"ett\":\"1577224033117\",\"en\":\"favorites\",\"kv\":{\"course_id\":3,\"id\":0,\"add_time\":\"1577161197714\",\"userid\":1}},{\"ett\":\"1577152037654\",\"en\":\"praise\",\"kv\":{\"target_id\":8,\"id\":7,\"type\":2,\"add_time\":\"1577203930964\",\"userid\":5}}]}";
        String x = new BaseFieldUDF().evaluate(line, "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }


}
