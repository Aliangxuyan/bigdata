package com.atguiugu.mapreduce.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by lxy on 2018/8/8.
 */
public class OrderSortPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean key, NullWritable nullWritable, int numReduceTasks) {
        return (key.getOrder_id() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
