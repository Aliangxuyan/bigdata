package com.lxy.ct.analysis;

import com.lxy.ct.analysis.tool.AnalysisBeanTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author lxy
 * @date 2019-12-16
 * 分析数据
 */
public class AnalysisData {
    public static void main(String[] args) throws Exception {
//        int result = ToolRunner.run(new AnalysisTextTool(), args);
        int result = ToolRunner.run(new AnalysisBeanTool(), args);
    }
}
