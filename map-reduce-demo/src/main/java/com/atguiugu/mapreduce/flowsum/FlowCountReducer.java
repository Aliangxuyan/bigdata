package com.atguiugu.mapreduce.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by lxy on 2018/7/31.
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context)
                throws IOException, InterruptedException {

            long sum_upFlow = 0;
            long sum_downFlow = 0;

            // 1 遍历所用bean，将其中的上行流量，下行流量分别累加
            for (FlowBean flowBean : values) {
                sum_upFlow += flowBean.getSumFlow();
                sum_downFlow += flowBean.getDownFlow();
            }

            // 2 封装对象
            FlowBean resultBean = new FlowBean(sum_upFlow, sum_downFlow);

            // 3 写出
            context.write(key, resultBean);
        }
    }
