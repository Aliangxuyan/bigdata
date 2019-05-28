package com.atguigu.function.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by lxy on 2018/8/12.
 */
public class Lower extends UDF {
    public String evaluate (final String s) {

        if (s == null) {
            return null;
        }

        return s.toLowerCase();
    }

}
