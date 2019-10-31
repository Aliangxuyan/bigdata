package com.atguiugu.mapreduce.shuffle.writablecomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by lxy on 2018/8/8.
 * <p>
 * 排序是针对K 排序,是一个对象，在输出的时候Key  value  调换位置
 */
public class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 循环输出，避免总流量相同情况
        for (Text text : values) {
            context.write(text, key);
        }

    }
}
