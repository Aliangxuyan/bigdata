package com.lxy.common.constant;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * @author lxy
 * @date 2019-12-15
 * <p>
 * 常量的另外一种方式，用配置文件
 */
public class ConfigConstant {
    private static Map<String, String> valueMap = new HashMap<String, String>();

    static {
        //国际化
        ResourceBundle ct = ResourceBundle.getBundle("ct");
        Enumeration<String> keys = ct.getKeys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            String val = ct.getString(key);
            valueMap.put(key, val);
        }
    }

    public static String getValue(String key) {
        return valueMap.get(key);
    }

    public static void main(String[] args) {
        System.out.println(ConfigConstant.getValue("ct.namespace"));
    }
}
