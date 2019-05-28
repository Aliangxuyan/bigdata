package com.atguiugu.mapreduce.serializablebean;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 注意：反序列化的顺序和序列化的顺序完全一致
 * Created by lxy on 2018/7/31.
 *
 * 1：必须实现Writable接口
 * 2：反序列化时，需要反射调用空参构造函数，所以必须有空参构造
 * 3：重写序列化方法
 * 4：重写反序列化方法
 * 5：反序列化的顺序和序列化的顺序完全一致
 * 6：要想把结果显示在文件中，需要重写toString()，可用”\t”分开，方便后续用
 * 7：如果需要将自定义的bean放在key中传输，则还需要实现comparable接口，因为mapreduce框中的shuffle过程一定会对key进行排序
 *
 */
@Data
public class FlowBean implements Writable,Comparable<FlowBean>{
    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    public FlowBean(){
        super();
    }
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

    @Override
    public int compareTo(FlowBean o) {
        // 倒序排列，从大到小
        return this.sumFlow > o.getSumFlow() ? -1 : 1;
    }
}
