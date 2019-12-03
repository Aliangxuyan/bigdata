package com.atguigu.hbase.mr2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-11-29
 * 防止数据偏移，reducer 端工作量太大，直接在mapper  端做一些过滤，直接传put 给reducer
 */
public class Fruit2Mapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper.Context context) throws IOException, InterruptedException {
        // 构建put 对象
        Put put = new Put(key.get());
        // 1、获取数据
        for (Cell cell : value.rawCells()) {
            //2、判断数据是否为name 列
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                put.add(cell);
            }
        }
        context.write(key, put);
    }
}
