package com.lxy.common.bean;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * @author lxy
 * @date 2019-11-18
 */
public interface DataIN extends Closeable {

    public void setPath(String path);

    public Object read() throws IOException;

    public <T extends Data> List<T> read(Class<T> clazz) throws IOException;
}
