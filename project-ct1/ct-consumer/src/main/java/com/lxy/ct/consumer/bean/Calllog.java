package com.lxy.ct.consumer.bean;

import com.lxy.common.api.Column;
import com.lxy.common.api.Rowkey;
import com.lxy.common.api.TableRef;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lxy
 * @date 2019-12-16
 * 通话日志
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@TableRef("ct:calllog")
public class Calllog {
    @Rowkey
    private String rowkey;
    @Column(family = "caller")
    private String call1;
    @Column(family = "caller")
    private String call2;
    @Column(family = "caller")
    private String calltime;
    @Column(family = "caller")
    private String duration;
    @Column(family = "caller")
    private String flag = "1"; // 标示主叫和被叫，存储两份数据对于大数据存储没有什么问题，但是查询会存在性能问题，所以可以直接使用不同列族存储

    private String name;

    public Calllog(String data) {
        String[] values = data.split("\t");
        call1 = values[0];
        call2 = values[1];
        calltime = values[2];
        duration = values[3];
    }
}
