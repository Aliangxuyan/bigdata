package com.lxy.common.constant;

import com.lxy.common.bean.Val;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * @author lxy
 * @date 2019-11-18
 * 名称常量枚举类型
 */
@NoArgsConstructor
@AllArgsConstructor
public enum Names implements Val {
    NAMESPACE("ct"),
    TOPIC("ct"),
    TABLE("ct:calllog"),
    CF_CALLER("caller"),
    CF_CALLEE("callee"),
    CF_INFO("info");

    private String name;

    public String value() {
        return name;
    }

    public void setValue(Object val) {
        this.name = (String) val;
    }

    public String getValue() {
        return name;
    }
}
