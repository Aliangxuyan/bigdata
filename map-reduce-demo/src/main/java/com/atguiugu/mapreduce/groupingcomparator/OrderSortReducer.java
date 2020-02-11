package com.atguiugu.mapreduce.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by lxy on 2018/8/8.
        */
public class OrderSortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

        context.write(key, NullWritable.get());
    }
}