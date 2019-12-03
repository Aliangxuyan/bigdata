package com.atguigu.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author lxy
 * @date 2019-11-29
 *
 * /opt/bigdata/hadoop-2.7.2/bin/yarn jar /Users/lxy/Documents/Idea_workspace/bigdata/hbase-demo/target/hbase-demo-1.0-SNAPSHOT.jar com.atguigu.hbase.mr1.FruitDriver /hbase/tmp/fruit.tsv fruit3
 *
 * /hbase/tmp/fruit.tsv fruit3 是两个定义的穿参
 *
 * 通过mapper-reducer往 hbase 写入数据
 *
 * yarn 执行jar 包,mr 网hbase  写数据
 *
 * 打包运行，参数可以外面传过来，但是本地运行需要一些配置，见 mr2
 */
public class FruitDriver implements Tool {

    // 1、定义一个Configuration
    private Configuration configuration = null;


    public int run(String[] args) throws Exception {
        //1、获取job对象
        Job job = Job.getInstance(configuration);
        //2、设置驱动类路径
        job.setJarByClass(FruitDriver.class);

        //3、设置mapper & mapper 输出的kv 类型
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //4、设置reducer
//        job.setReducerClass(FruitReducer.class);
        TableMapReduceUtil.initTableReducerJob(args[1], FruitReducer.class, job);
        //5、设置最终的输出数据类型
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //6、提交任务
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            int run = ToolRunner.run(configuration, new FruitDriver(), args);

            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setConf(Configuration conf) {
        configuration = conf;
    }

    public Configuration getConf() {
        return configuration;
    }
}
