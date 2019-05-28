package com.atguiugu.mapreduce.inputformat.userdefinedformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by lxy on 2018/8/1.
 */
public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private Configuration configuration;
    private FileSplit split;

    private boolean processed = false;
    private BytesWritable value = new BytesWritable();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        this.split = (FileSplit)split;
        configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!processed) {
            // 1 定义缓存区
            byte[] contents = new byte[(int) split.getLength()];

            FileSystem fs = null;
            FSDataInputStream fis = null;

            try {
                // 2 获取文件系统
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);

                // 3 读取数据
                fis = fs.open(path);

                // 4 读取文件内容
                IOUtils.readFully(fis, contents, 0, contents.length);

                // 5 输出文件内容
                value.set(contents, 0, contents.length);
            } catch (Exception e) {

            } finally {
                IOUtils.closeStream(fis);
            }

            processed = true;
        }
        return processed;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
