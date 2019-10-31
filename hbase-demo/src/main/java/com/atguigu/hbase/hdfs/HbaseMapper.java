package com.atguigu.hbase.hdfs;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * hdfs 到 hbase
 */
public class HbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private ImmutableBytesWritable k = new ImmutableBytesWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        k.set(Bytes.toBytes(split[0]));
        Put put = new Put(Bytes.toBytes(split[0]));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(split[1]));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(split[2]));
        context.write(k, put);
    }
}
