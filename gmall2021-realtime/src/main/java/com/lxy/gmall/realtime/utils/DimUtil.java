package com.lxy.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

/**
 * @author lxy
 * @date 2021/3/17
 * 查询维度的工具类
 */
public class DimUtil {
    //直接从Phoenix查询，没有缓存
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... colNameAndValue) {
        //组合查询条件
        String wheresql = new String(" where ");
        for (int i = 0; i < colNameAndValue.length; i++) {
            //获取查询列名以及对应的值
            Tuple2<String, String> nameValueTuple = colNameAndValue[i];
            String fieldName = nameValueTuple.f0;
            String fieldValue = nameValueTuple.f1;
            if (i > 0) {
                wheresql += " and ";
            }
            wheresql += fieldName + "='" + fieldValue + "'";
        }
        //组合查询SQL
        String sql = "select * from " + tableName + wheresql;
        System.out.println("查询维度SQL:" + sql);
        JSONObject dimInfoJsonObj = null;
        List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
        if (dimList != null && dimList.size() > 0) {
            //因为关联维度，肯定都是根据key关联得到一条记录
            dimInfoJsonObj = dimList.get(0);
        } else {
            System.out.println("维度数据未找到:" + sql);
        }
        return dimInfoJsonObj;
    }

    public static void main(String[] args) {
        JSONObject dimInfooNoCache = DimUtil.getDimInfoNoCache("base_trademark", Tuple2.of("id", "13"));
        System.out.println(dimInfooNoCache);
    }
}

