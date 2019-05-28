package com.atguigu.compress.map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {

        int count = 0;
        // 1 汇总
        for(IntWritable value:values){
            count += value.get();
        }

        // 2 输出
        context.write(key, new IntWritable(count));
    }
}