package com.lxy.mr;

/**
 * @author lxy
 * @date 2019-11-25
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VideoETLRunner implements Tool {

    private Configuration conf = null;

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public int run(String[] args) throws Exception {
        // 1、获取配置信息对象以及封装任务
        // Configuration conf = new Configuration();
        Job job = Job.getInstance(getConf());

        // 2、设置jar的加载路径
        job.setJarByClass(VideoETLRunner.class);

        // 3、设置map和reduce类
        job.setMapperClass(VideoETLMapper.class);
        // job.setReducerClass(WordcountReducer.class);

        // 4、设置map输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5、设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 6、设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 因为这里我们不使用Reduce
        job.setNumReduceTasks(0);

        // 7、提交job
        // job.submit();
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public static void main(String[] args) {
        int resultCode = 0;
        try {
            resultCode = ToolRunner.run(new VideoETLRunner(), args);
            if (resultCode == 0) {
                System.out.println("Success!");
            } else {
                System.out.println("Fail!");
            }
            System.exit(resultCode);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}

