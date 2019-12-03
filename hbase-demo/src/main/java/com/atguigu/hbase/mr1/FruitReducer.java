package com.atguigu.hbase.mr1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-11-29
 */
public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // 一些变量可以在这定义
        super.setup(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //1、遍历values
        for (Text value : values) {

            // 2、获取每一行数据
            String[] split = value.toString().split("\t");

            // 3、构建put 对象
            Put put = new Put(Bytes.toBytes(split[0]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(split[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(split[2]));

            context.write(NullWritable.get(), put);

        }
    }
}
