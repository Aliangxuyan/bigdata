package com.lxy.rpc;

/**
 * @author lxy
 * @date 2021-07-22
 */
public interface RPCProtocol {

    long versionID = 666;

    void mkdirs(String path);
}