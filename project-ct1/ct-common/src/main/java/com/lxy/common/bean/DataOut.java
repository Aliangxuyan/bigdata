package com.lxy.common.bean;

import java.io.Closeable;

/**
 * @author lxy
 * @date 2019-11-18
 */
public interface DataOut extends Closeable {
    public void setPath(String path);

    public void write(Object data) throws Exception;

    public void write(String data) throws Exception;
}
