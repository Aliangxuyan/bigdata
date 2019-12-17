package com.lxy.ct.analysis.tool;

import com.lxy.common.constant.Names;
import com.lxy.ct.analysis.io.MYSQLBeanOutputFormat;
import com.lxy.ct.analysis.kv.AnalysisKey;
import com.lxy.ct.analysis.kv.AnalysisValue;
import com.lxy.ct.analysis.mapper.AnalysisBeanMapper;
import com.lxy.ct.analysis.reducer.AnalysisBeanReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * @author lxy
 * @date 2019-12-16
 * 分析数据的工具类
 */
public class AnalysisBeanTool implements Tool {
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(AnalysisBeanTool.class);

        //扫描数据
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.value()));

        // mapper
        TableMapReduceUtil.initTableMapperJob(
                Names.TABLE.getValue(),
                scan,
                AnalysisBeanMapper.class,
                AnalysisKey.class,
                AnalysisValue.class,
                job
        );

        //reducer
        job.setReducerClass(AnalysisBeanReducer.class);
        job.setOutputKeyClass(AnalysisKey.class);
        job.setOutputValueClass(AnalysisValue.class);

        //outputformat
        job.setOutputFormatClass(MYSQLBeanOutputFormat.class);

        boolean flag = job.waitForCompletion(true);
        if (flag) {
            return JobStatus.State.SUCCEEDED.getValue();
        } else {
            return JobStatus.State.FAILED.getValue();
        }
    }

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}
