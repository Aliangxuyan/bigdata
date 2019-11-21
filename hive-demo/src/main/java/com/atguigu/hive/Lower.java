package com.atguigu.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author lxy
 * @date 2019-11-20
 * <p>
 * 自定义hive  函数
 */
public class Lower extends UDF {
    public String evaluate(final String s) {

        if (s == null) {
            return null;
        }

        return s.toLowerCase();
    }
}
