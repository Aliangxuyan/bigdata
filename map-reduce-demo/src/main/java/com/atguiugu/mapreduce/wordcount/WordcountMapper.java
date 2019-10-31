package com.atguiugu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * mapper类
 * Created by lxy on 2018/7/31.
 *
 * KEYIN: 输入数据的Key
 * VALUEIN: 输入数据的value
 * KEYOUT:  输出的数据的key  类型 atguigu:1  ss:2
 * VALUEOUT: 输出的数据的value 类型
 *
 */
public class WordcountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();

        // 2 切割
        String[] words = line.split(" ");

        // 3 输出
        for (String word : words) {

            k.set(word);
            context.write(k, v);
        }
    }
}
