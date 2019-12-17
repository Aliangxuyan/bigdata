package com.lxy.common.utils;

import java.text.DecimalFormat;

/**
 * @author lxy
 * @date 2019-12-15
 * 数字工具类
 */
public class NumberUtil {

    public static String format(int num, int lenth) {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < lenth; i++) {
            stringBuffer.append("0");
        }
        DecimalFormat df = new DecimalFormat(stringBuffer.toString());
        return df.format(num);
    }

    public static void main(String[] args) {
        System.out.println(format(10, 10));
    }
}
