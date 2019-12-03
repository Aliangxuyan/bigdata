package com.lxy.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @author lxy
 * @date 2019-12-01
 */
public class Constants {

    // Hbase 的配置嘻嘻
    public static final Configuration CONFIGURATION = HBaseConfiguration.create();

    // 命名空间
    public static final String NAMESPACE = "weibo";

    // 微博内容表
    public static final String CONTENT_TABLE = "weibo:content";
    public static final String CONTENT_TABLE_CF = "info";
    public static final int CONTENT_TABLE_VERSIONs = 1;


    // 用户关系表
    public static final String RALATION_TABLE = "weibo:relation";
    public static final String RALATION_TABLE_CF1 = "attends";
    public static final String RALATION_TABLE_CF2 = "fans";
    public static final int RALATION_TABLE_VERSIONs = 1;

    // 收件箱表
    public static final String INBOX_TABLE = "weibo:inbox";
    public static final String INBOX_TABLE_CF = "info";
    public static final int INBOX_TABLE_VERSIONs = 2;

}
