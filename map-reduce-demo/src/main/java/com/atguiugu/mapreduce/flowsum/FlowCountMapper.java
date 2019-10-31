package com.atguiugu.mapreduce.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by lxy on 2018/7/31.
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

        FlowBean v = new FlowBean();
        Text k = new Text();
        int i = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // 根据文件类型获取切片信息
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            // 获取切片的文件名称
            String name = inputSplit.getPath().getName();

            System.out.print("**********************根据文件类型获取切片信息  name :" +name + i++ +"\n" );

            // 1 获取一行
            String line = value.toString();

            // 2 切割字段
            String[] fields = line.split("\t");

            // 3 封装对象
            // 取出手机号码
            String phoneNum = fields[1];
            k.set(phoneNum);

            // 取出上行流量和下行流量
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);

            v.setDownFlow(downFlow);
            v.setUpFlow(upFlow);
//            FlowBean flowBean = new FlowBean(upFlow, downFlow);

            // 4 写出
            context.write(k, v);
        }
    }
