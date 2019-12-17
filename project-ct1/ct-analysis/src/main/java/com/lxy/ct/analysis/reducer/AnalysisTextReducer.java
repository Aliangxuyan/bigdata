package com.lxy.ct.analysis.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-12-16
 * <p>
 * 分析数据的reducer
 */
public class AnalysisTextReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumCall = 0;
        int sumDuration = 0;

        for (Text value : values) {
            int duration = Integer.parseInt(value.toString());
            sumDuration += duration;
            sumCall++;
        }
        context.write(key, new Text(sumCall + "_" + sumDuration));
    }
}
