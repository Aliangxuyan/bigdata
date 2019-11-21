package com.atguiugu.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by lxy on 2018/8/8.
 * Combiner 的输出应该和 reducer 的输入的 k v 类型对应起来
 *
 * 不适合求平均值的情景，汇总可以
 */
public class WordcountCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        // 1 汇总操作
        int count = 0;
        for(IntWritable v :values){
            count = v.get();
        }
        // 2 写出
        context.write(key, new IntWritable(count));
    }

}
