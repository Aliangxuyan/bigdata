package com.lxy.mr;

/**
 * @author lxy
 * @date 2019-11-25
 */

import com.lxy.utils.ETLUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VideoETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1.获取一行数据
        String ori = value.toString();

        // 2.清洗数据
        String etlString = ETLUtil.oriString2ETLString(ori);

        // 3.写出
        if (StringUtils.isBlank(etlString)) {
            return;
        }
        k.set(etlString);
        context.write(k, NullWritable.get().get());
    }
}

