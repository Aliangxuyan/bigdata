package com.atguiugu.mapreduce.shuffle.writablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by lxy on 2018/8/8.
 */
public class FlowCountSortReducer extends Reducer<Text ,FlowBean,Text,FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 循环输出，避免总流量相同情况
        for (FlowBean text : values) {
            context.write(key, text);
        }

    }
}
