package com.lxy.gmall.realtime.app.udf;

import com.lxy.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author lxy
 * @date 2021/3/18
 *
 * 自定义UDTF函数实现分词功能
 * @FunctionHint 主要是为了标识输出数据的类型
 */
@FunctionHint(output = @DataTypeHint("ROW<s STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String value) {
        List<String> keywordList = KeywordUtil.analyze(value);
        for (String keyword : keywordList) {
            Row row = new Row(1);
            row.setField(0,keyword);
            collect(row);
        }
    }
}