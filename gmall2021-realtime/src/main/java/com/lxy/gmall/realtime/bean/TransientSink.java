package com.lxy.gmall.realtime.bean;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author lxy
 * @date 2021/3/18
 * 向ClickHouse写入数据的时候，如果有字段数据不需要传输，可以用该注解标记
 */
@Target(FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
