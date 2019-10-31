package com.atguiugu.mapreduce.shuffle.writablecomparable;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lxy on 2018/8/7.
 * <p>
 * 按流量排序  实现 WritableComparable 接口
 *
 * <p>
 * 3.3.4 WritableComparable排序
 */
@Data
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    // 反序列化时，需要反射调用空参构造函数，所以必须有
    public FlowBean() {
        super();
    }

    @Override
    public int compareTo(FlowBean flowBean) {
        int result;
        // 核心比较条件代码
        if (sumFlow > flowBean.sumFlow) {
            result = 1;
        } else if (sumFlow < flowBean.sumFlow) {
            result = -1;
        } else {
            result = 0;
        }
        return result;
    }

    /**
     * write 和 readFields 的顺序必须保持一致
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    public FlowBean(long upFlow, long downFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" +
                downFlow + "\t" +
                sumFlow;
    }
}
