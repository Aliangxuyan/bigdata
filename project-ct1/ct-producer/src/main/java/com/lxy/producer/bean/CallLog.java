package com.lxy.producer.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lxy
 * @date 2019-12-15
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CallLog {
    private String call1;
    private String call2;
    private String calltime;
    private String duration;

    @Override
    public String toString() {
        return call1 + "\t" + call2 + "\t" + calltime + "\t" + duration;
    }
}
