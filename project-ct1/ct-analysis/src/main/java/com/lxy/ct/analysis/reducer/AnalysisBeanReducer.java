package com.lxy.ct.analysis.reducer;

import com.lxy.ct.analysis.kv.AnalysisKey;
import com.lxy.ct.analysis.kv.AnalysisValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-12-16
 * <p>
 * 分析数据的reducer
 */
public class AnalysisBeanReducer extends Reducer<AnalysisKey, Text, AnalysisKey, AnalysisValue> {

    @Override
    protected void reduce(AnalysisKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumCall = 0;
        int sumDuration = 0;

        for (Text value : values) {
            int duration = Integer.parseInt(value.toString());
            sumDuration += duration;
            sumCall++;
        }
        context.write(key, new AnalysisValue("" + sumCall, "" + sumDuration));
    }
}
