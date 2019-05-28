package com.atguiugu.mapreduce.inputformat.keyvaluetextinputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KeyValueTextInputFormat
 * Created by lxy on 2018/7/31.
 */
public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable> {

    final Text k = new Text();
    final LongWritable v = new LongWritable();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // banzhang ni hao
        // 1 设置key和value
        // banzhang
        k.set(key);
        // 设置key的个数
        v.set(1);

        // 2 写出
        context.write(k, v);
    }
}
