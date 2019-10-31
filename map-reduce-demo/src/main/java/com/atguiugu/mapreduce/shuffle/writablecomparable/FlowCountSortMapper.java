package com.atguiugu.mapreduce.shuffle.writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by lxy on 2018/8/7.
 */
public class FlowCountSortMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    FlowBean v = new FlowBean();
    Text k= new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();

        // 2 截取
        String[] fields = line.split(",");

        // 3 封装对象
        String phoneNbr = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);

        v.set(upFlow, downFlow);
        k.set(phoneNbr);

        // 4 输出
        context.write(k, v);

    }
}
