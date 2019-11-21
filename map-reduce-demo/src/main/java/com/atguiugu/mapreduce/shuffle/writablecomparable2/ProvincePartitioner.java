package com.atguiugu.mapreduce.shuffle.writablecomparable2;

import com.atguiugu.mapreduce.shuffle.writablecomparable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by lxy on 2018/8/8.
 *
 * 自定义 Partitioner 类，默认是
 *
 * public class HashPartitioner<K, V> extends Partitioner<K, V> {
 *
 *   /** Use {@link Object#hashCode()} to partition.
    *public int getPartition(K key,V value,
        *int numReduceTasks){
        *return(key.hashCode()&Integer.MAX_VALUE)%numReduceTasks;
        *}
        *
        *}
 *
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean key, Text value, int numPartitions) {

        // key 是手机号，value  是流量信息®

        // 1 获取手机号码前三位
        String preNum = key.toString().substring(0, 3);

        int partition = 4;

        // 2 根据手机号归属地设置分区
        if ("136".equals(preNum)) {
            partition = 0;
        } else if ("137".equals(preNum)) {
            partition = 1;
        } else if ("138".equals(preNum)) {
            partition = 2;
        } else if ("139".equals(preNum)) {
            partition = 3;
        }

        return partition;
    }
}