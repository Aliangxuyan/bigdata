package com.lxy.producer.io;

import com.lxy.common.bean.DataOut;

import java.io.*;

/**
 * @author lxy
 * @date 2019-12-15
 */
public class LocalFileDateOut implements DataOut {
    private PrintWriter writer = null;

    public LocalFileDateOut(String path) {
        setPath(path);
    }

    public void setPath(String path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void write(Object data) throws Exception {
        write(data.toString());
    }

    /**
     * 将数据字符串生成到文件中
     *
     * @param data
     * @throws Exception
     */
    public void write(String data) throws Exception {
        writer.println(data);
        writer.flush();
    }

    public void close() throws IOException {
    if (writer != null){
        writer.close();
    }
    }
}
