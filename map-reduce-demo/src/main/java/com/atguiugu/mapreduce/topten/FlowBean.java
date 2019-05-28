package com.atguiugu.mapreduce.topten;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {

    private Long sumFlow; // 总流量
    private String phoneNum; // 手机号

    // 空参构造
    public FlowBean() {
        super();
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    @Override
    public String toString() {
        return sumFlow + "\t" + phoneNum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // 序列化
        out.writeLong(sumFlow);
        out.writeUTF(phoneNum);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // 反序列化
        this.sumFlow = in.readLong();
        this.phoneNum = in.readUTF();

    }

    @Override
    public int compareTo(FlowBean o) {
        int result;

        result = this.sumFlow.compareTo(o.sumFlow);
        if (result == 0) {
            result = this.phoneNum.compareTo(o.phoneNum);
        }
        return result;
    }
}