package com.lxy.common.bean;

/**
 * @author lxy
 * @date 2019-11-18
 */
public class Data implements Val {
    public String content;

    public void setValue(String value) {
        content = value;
    }


    public Object value() {
        return null;
    }
}
