package com.atguiugu.mapreduce.inputformat.nlineinputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by lxy on 2018/7/31.
 */
public class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    LongWritable v = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;

        // 1 汇总
        for (LongWritable value:values){
            count += value.get();
        }
        v.set(count);

        // 2 输出
        context.write(key,v);
    }
}
