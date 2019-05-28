package com.atguiugu.mapreduce.topten;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // 1.定义一个TreeMap作为存储数据的容器（天然按key排序）
        TreeMap<FlowBean, Text> flowMap = new TreeMap<FlowBean, Text>();

        for (Text value : values) {
            FlowBean bean = new FlowBean();
            bean.setPhoneNum(value.toString().split("\t")[0]);
            bean.setSumFlow(Long.parseLong(value.toString().split("\t")[3]));
            flowMap.put(bean, new Text(value));

            // 2.限制TreeMap的数据量，超过10条就删除掉流量最小的一条数据
            if (flowMap.size() > 10) {
                flowMap.remove(flowMap.firstKey());
            }
        }

        // 3.输出
        for (Text t : flowMap.descendingMap().values()) {
            context.write(NullWritable.get(), t);
        }
    }
}