package com.lxy.ct.analysis.io;

import com.lxy.common.utils.JDBCUtil;
import com.lxy.ct.analysis.kv.AnalysisKey;
import com.lxy.ct.analysis.kv.AnalysisValue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


/**
 * @author lxy
 * @date 2019-12-16
 */
public class MYSQLBeanOutputFormat extends OutputFormat<AnalysisKey, AnalysisValue> {
    private FileOutputCommitter committer = null;


    protected static class MySQLRecordWriter
            extends RecordWriter<AnalysisKey, AnalysisValue> {


        private Connection connection = null;
        private Jedis jedis = null;

        private Map<String, Integer> userMap = new HashMap<String, Integer>();
        private Map<String, Integer> dataMap = new HashMap<String, Integer>();

        /**
         * 获取资源
         */
        public MySQLRecordWriter() {
            // 获取资源
            connection = JDBCUtil.getConnection();
            jedis = new Jedis("localhost", 6379);

        }

        /**
         * 输出数据
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        public void write(AnalysisKey key, AnalysisValue value) throws IOException, InterruptedException {
            PreparedStatement psStat = null;
            try {
                String sql = "insert into ct_call ( telid, dateid, sumcall, sumduration ) values ( ?, ?, ?, ? )";

                psStat = connection.prepareStatement(sql);
                psStat.setInt(1, Integer.parseInt(jedis.hget("ct_user", key.getTel())));
                psStat.setInt(2, Integer.parseInt(jedis.hget("ct_date", key.getDate())));
                psStat.setInt(3, Integer.parseInt(value.getSumCall()));
                psStat.setInt(4, Integer.parseInt(value.getSumDuration()));

                psStat.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (psStat != null) {
                    try {
                        psStat.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * 释放资源
         *
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }

    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    /**
     * 不使用，不报错就可以，参考 TextOutputFormat FileOutputFormat 类中的
     *
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(taskAttemptContext);
            committer = new FileOutputCommitter(output, taskAttemptContext);
        }
        return committer;
    }

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }
}
