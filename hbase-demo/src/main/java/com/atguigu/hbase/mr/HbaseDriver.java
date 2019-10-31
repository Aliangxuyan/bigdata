package com.atguigu.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-05-31
 */
public class HbaseDriver {
    public static void main(String[] args) throws IOException {

        // 1、后去Hbase 的conf & 封装job
        Configuration configuration = new Configuration();
        Configuration conf = HBaseConfiguration.create(configuration);

        Job job = Job.getInstance(conf);

        // 2、设置主类
        job.setJarByClass(HbaseDriver.class);

        // 3、设置mapper 类
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("fruit"),
                scan,
                HbaseMapper.class,
                ImmutableBytesWritable.class,
                Put.class, job);
        job.setMapperClass(HbaseMapper.class);

        // 4、
        job.setNumReduceTasks(0);
    }
}
