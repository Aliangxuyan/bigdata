package com.atguiugu.mapreduce.inputformat.keyvaluetextinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by lxy on 2018/7/31.
 */
public class MyDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 设置切割符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        // 获取job对象
        Job job = Job.getInstance(conf);

        // 设置jar包位置，关联mapper和reducer
        job.setJarByClass(MyDriver.class);
        job.setMapperClass(KVTextMapper.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置最终输出kv类型
        job.setReducerClass(KVTextReducer.class);
        job.setOutputKeyClass(Text.class);

        // 设置输入输出数据路径
        FileInputFormat.setInputPaths(job, new Path("map-reduce-demo/file/tmp/KVFormat.txt"));

        // 设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 设置输出数据路径
        FileOutputFormat.setOutputPath(job, new Path("map-reduce-demo/file/tmp/KVFormat_out.txt"));

        // 提交job
        job.waitForCompletion(true);
    }
}

