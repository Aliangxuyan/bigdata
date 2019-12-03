package com.atguigu.hbase.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-05-31
 */
public class HbaseDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Configuration conf = HBaseConfiguration.create(configuration);

        Job job = Job.getInstance(conf);
        job.setJarByClass(HbaseDriver.class);

        job.setMapperClass(HbaseMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob(args[1], HbaseReducer.class, job);
        Path path = new Path(args[0]);
        FileInputFormat.setInputPaths(job, path);

        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);

    }

}
