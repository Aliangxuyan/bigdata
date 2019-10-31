package com.atguiugu.mapreduce.inputformat.userdefinedformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by lxy on 2018/8/1.
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {
    private Configuration configuration;
    private FileSplit split;

    Text k = new Text();
    BytesWritable v = new BytesWritable();

    private boolean isProcessed = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 初始化
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // 核心业务逻辑处理
        if (isProcessed) {
            // 定义缓存区
            byte[] buf = new byte[(int) split.getLength()];

            FileSystem fs = null;
            FSDataInputStream fis = null;

            try {
                // 1 获取文件系统，fs 对象
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);

                // 2 读取数据
                fis = fs.open(path);

                // 3 拷贝
                IOUtils.readFully(fis, buf, 0, buf.length);

                // 4 封装V
                v.set(buf, 0, buf.length);

                // 5 封装K
                k.set(path.toString());
            } catch (Exception e) {

            } finally {
                // 6 关闭资源
                IOUtils.closeStream(fis);
            }

            isProcessed = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {

        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
