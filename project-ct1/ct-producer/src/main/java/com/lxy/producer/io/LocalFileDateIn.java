package com.lxy.producer.io;

import com.lxy.common.bean.Data;
import com.lxy.common.bean.DataIN;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lxy
 * @date 2019-12-15
 * <p>
 * 本地文件数据输入
 */
public class LocalFileDateIn implements DataIN {
    private BufferedReader reader = null;


    public LocalFileDateIn(String path) {
        setPath(path);
    }

    public void setPath(String path) {
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public Object read() throws IOException {
        return null;
    }

    /**
     * 读取数据返回数据集合
     *
     * @param clazz
     * @param <T>
     * @return
     * @throws IOException
     */
    public <T extends Data> List<T> read(Class<T> clazz) throws IOException {

        List<T> ts = new ArrayList<T>();

        // 从数据文件中读取所有的数据
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                //将数据转化为制定类型的对象，封装为集合返回
                T t = clazz.newInstance();
                t.setValue(line);
                ts.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ts;
    }

    /**
     * @throws IOException 关闭资源
     */
    public void close() throws IOException {
        if (null != reader) {
            reader.close();
        }
    }
}
