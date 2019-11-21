package com.lxy.common.constant;

import com.lxy.common.bean.Val;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * @author lxy
 * @date 2019-11-18
 * 名称常量枚举类型
 */
@AllArgsConstructor
@NoArgsConstructor
public enum Names implements Val {
    NAMESPACE("ct");

    private String name;

    public Object value() {
        return name;
    }
}
