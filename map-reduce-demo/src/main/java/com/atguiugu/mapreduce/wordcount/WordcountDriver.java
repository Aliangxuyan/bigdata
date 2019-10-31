package com.atguiugu.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by lxy on 2018/7/31.
 */
public class WordcountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(WordcountDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置Reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置两个分区测试partition
        job.setNumReduceTasks(2);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("map-reduce-demo/file/tmp/hello.txt"));
        FileOutputFormat.setOutputPath(job,new Path("map-reduce-demo/file/tmp/hello_out.txt"));


// 测试处理多文件分片数,示例中默认是number of splits:3  合并之后 number of splits:1
//*************************************************** CombineTextInputFormat  start********************************************************************

//        //如果不设置 InputFormat 默认是 TextInputFormat
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job,1024); // 1K
//
//        FileInputFormat.setInputPaths(job,new Path("map-reduce-demo/file/tmp/combine_input"));
//        FileOutputFormat.setOutputPath(job,new Path("map-reduce-demo/file/tmp/combine_output"));

//*************************************************** CombineTextInputFormat end ********************************************************************
        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 :1);
    }
}
