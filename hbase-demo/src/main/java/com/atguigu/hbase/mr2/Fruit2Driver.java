package com.atguigu.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author lxy
 * @date 2019-11-29
 * <p>
 * 功能：hbase  从fruit3 表中name 列查到 hbase fruit4 表中，
 *
 * 最终验证：
 *
 * hbase(main):005:0> scan 'fruit4'
 * ROW                                                  COLUMN+CELL
 *  1001                                                column=info:name, timestamp=1575010735389, value=Apple
 *  1002                                                column=info:name, timestamp=1575010735389, value=Pear
 *  1003                                                column=info:name, timestamp=1575010735389, value=Pineapple
 */
public class Fruit2Driver implements Tool {
    Configuration configuration = new Configuration();

    public static void main(String[] args) {
        try {
//            Configuration configuration = new Configuration();
            /**
             * 源码追踪结果：
             * HBaseConfiguration.create -> addHbaseResources(conf) ->  conf.addResource("hbase-site.xml");
             *
             * 本地运行的方法：
             *  1、上面直接xml 文件形式
             *  2、conf 设置传值
             */
            Configuration configuration = HBaseConfiguration.create();
            ToolRunner.run(configuration, new Fruit2Driver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int run(String[] args) throws Exception {
        //1、获取job对象
        Job job = Job.getInstance(configuration);
        //2、设置驱动类路径
        job.setJarByClass(Fruit2Driver.class);
        //3、设置mapper & mapper 输出的kv 类型
        Scan scan = new Scan();
//        TableMapReduceUtil.initTableMapperJob(args[0],
        TableMapReduceUtil.initTableMapperJob("fruit3",
                scan,
                Fruit2Mapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);

        //4、设置reducer
//        TableMapReduceUtil.initTableReducerJob(args[1], Fruit2Reducer.class, job);
        TableMapReduceUtil.initTableReducerJob("fruit4",
                Fruit2Reducer.class,
                job);
        //6、提交任务
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public void setConf(Configuration conf) {
        configuration = conf;
    }

    public Configuration getConf() {
        return configuration;
    }
}
