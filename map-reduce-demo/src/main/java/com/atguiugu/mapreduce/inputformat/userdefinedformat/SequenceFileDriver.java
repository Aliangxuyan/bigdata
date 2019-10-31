package com.atguiugu.mapreduce.inputformat.userdefinedformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created by lxy on 2018/8/1.
 */
public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] { "map-reduce-demo/file/tmp/sequence", "map-reduce-demo/file/tmp/sequence_output" };
        Configuration conf = new Configuration();

        // 1、获取JOb
        Job job = Job.getInstance(conf);

        // 2、设置jar  包存储位置 关联自定义mapper  和reducer
        job.setJarByClass(SequenceFileDriver.class);

        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        // 3、设置输入的inputFormat
        job.setInputFormatClass(WholeFileInputformat.class);

        // 4、 设置输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 5、设置mapper输出端的kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 6、设置最终输出端的kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 7、设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8、提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
