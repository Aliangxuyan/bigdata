package com.lxy.common.bean;

/**
 * @author lxy
 * @date 2019-11-18
 */
public class Data implements Val {
    public String content;

    public void setValue(Object val) {
        content = (String) val;
    }

    public String getValue() {
        return content;
    }
}
