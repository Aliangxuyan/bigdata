package com.atguiugu.mapreduce.topten;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class TopTenMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    // 定义一个TreeMap作为存储数据的容器（天然按key排序）
    private TreeMap<FlowBean, Text> flowMap = new TreeMap<FlowBean, Text>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FlowBean bean = new FlowBean();
        // 1.获取一行
        String line = value.toString();

        // 2.切割
        String[] fields = line.split("\t");

        long sumFlow = Long.parseLong(fields[3]);

        bean.setSumFlow(sumFlow);
        bean.setPhoneNum(fields[0]);

        // 3.向TreeMap中添加数据
        flowMap.put(bean, new Text(value));

        // 4.限制TreeMap的数据量，超过10条就删除掉流量最小的一条数据
        if (flowMap.size() > 10) {
            flowMap.remove(flowMap.firstKey());
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 输出
        for (Text t : flowMap.values()) {
            context.write(NullWritable.get(), t);
        }
    }
}