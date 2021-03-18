package com.lxy.gmall.realtime.app.dws;

import com.lxy.gmall.realtime.app.udf.KeywordUDTF;
import com.lxy.gmall.realtime.bean.KeywordStats;
import com.lxy.gmall.realtime.common.GmallConstant;
import com.lxy.gmall.realtime.utils.ClickhouseUtil;
import com.lxy.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author lxy
 * @date 2021/3/18
 * 搜索关键字计算
 *
 * clickhouse
 *
 * create table keyword_stats_2021 (
 *     stt DateTime,
 *     edt DateTime,
 *     keyword String ,
 *     source String ,
 *     ct UInt64 ,
 *     ts UInt64
 * )engine =ReplacingMergeTree( ts)
 *         partition by  toYYYYMMDD(stt)
 *         order by  ( stt,edt,keyword,source );
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        /*
        //CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/ProvinceStatsSqlApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //TODO 1.定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //TODO 2.注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze",  KeywordUDTF.class);

        //TODO 3.将数据源定义为动态表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic ="dwd_page_log";


        tableEnv.executeSql("CREATE TABLE page_view " +
            "(common MAP<STRING,STRING>, " +
            "page MAP<STRING,STRING>,ts BIGINT, " +
            "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
            "WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '2' SECOND) " +
            "WITH ("+ MyKafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId)+")");

        //TODO 4.过滤数据
        Table fullwordView = tableEnv.sqlQuery("select page['item'] fullword ," +
            "rowtime from page_view  " +
            "where page['page_id']='good_list' " +
            "and page['item'] IS NOT NULL ");

        //TODO 5.利用udtf将数据拆分
        Table keywordView = tableEnv.sqlQuery("select keyword,rowtime  from " + fullwordView + " ," +
            " LATERAL TABLE(ik_analyze(fullword)) as T(keyword)");

        //TODO 6.根据各个关键词出现次数进行ct
        Table keywordStatsSearch  = tableEnv.sqlQuery("select keyword,count(*) ct, '"
            + GmallConstant.KEYWORD_SEARCH + "' source ," +
            "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
            "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
            "UNIX_TIMESTAMP()*1000 ts from   "+keywordView
            + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");


        //TODO 7.转换为数据流
        DataStream<KeywordStats> keywordStatsSearchDataStream =
            tableEnv.<KeywordStats>toAppendStream(keywordStatsSearch, KeywordStats.class);

        keywordStatsSearchDataStream.print();

        //TODO 8.写入到ClickHouse
        keywordStatsSearchDataStream.addSink(
            ClickhouseUtil.<KeywordStats>getJdbcSink(
                "insert into keyword_stats(keyword,ct,source,stt,edt,ts)  " +
                    " values(?,?,?,?,?,?)"));


        env.execute();
    }
}
