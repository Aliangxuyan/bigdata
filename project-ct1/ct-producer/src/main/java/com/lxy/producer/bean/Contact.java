package com.lxy.producer.bean;

import com.lxy.common.bean.Data;

/**
 * @author lxy
 * @date 2019-12-15
 */
@lombok.Data
public class Contact extends Data {
    private String tel;
    private String name;

    @Override
    public void setValue(Object val) {
        content = (String) val;
//        String[] values = content.split("\t");
        String[] values = content.split(",");
        setTel(values[0]);
        setName(values[1]);
    }

    @Override
    public String getValue() {
        return super.getValue();
    }

    @Override
    public String toString() {
        return "Contact [" + tel + ", " + name + "]";
    }
}
