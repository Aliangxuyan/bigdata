package com.lxy.gmall.realtime.common;

/**
 * @author lxy
 * @date 2021/3/15
 */
public class GmallConfig {
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop202,hadoop203,hadoop204:2181";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop202:8123/default";
}
