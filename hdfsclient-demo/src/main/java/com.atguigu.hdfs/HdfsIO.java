package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by lxy on 2018/7/30.
 */
public class HdfsIO {


    /**
     * HDFS文件上传
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lxy");

        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("file/hello.txt"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/user/lxy/map-reduce-demo/file/tmp/hello.txt"));

        // 4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    /**
     * 从hdfs 下载文件到本地磁盘
     */
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lxy");

        // 2 获取输人流
        FSDataInputStream fis = fs.open(new Path("/hello4.txt"));

        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("file/hello_from_hdfs.txt"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 定位文件读取
     *
     * 1、下载第一块
     *
     */
    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lxy");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("file/hadoop-2.7.2.tar.gz.part1"));

        // 4 流的拷贝
        byte[] buf = new byte[1024];

        for(int i =0 ; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }

        // 5关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 定位文件读取
     *
     * 2、下载第二块
     *
     */
    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lxy");

        // 2 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3 定位输入数据位置,指定读取的起点
        fis.seek(1024*1024*128);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("file/hadoop-2.7.2.tar.gz.part2"));

        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

}
