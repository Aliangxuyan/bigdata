package com.lxy.ct.analysis.kv;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author lxy
 * @date 2019-12-16
 * <p>
 * 自定义分析数据key
 */
@Data
public class AnalysisKey implements WritableComparable<AnalysisKey> {

    public AnalysisKey(String tel, String date) {
        this.tel = tel;
        this.date = date;
    }

    private String tel;
    private String date;

    /**
     * 比较,tel ,date
     *
     * @param key
     * @return
     */
    public int compareTo(AnalysisKey key) {
        int result = tel.compareTo(key.getTel());
        if (result == 0) {
            result = date.compareTo(key.getDate());
        }
        return result;
    }

    /**
     * 写数据
     *
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tel);
        dataOutput.writeUTF(date);
    }

    /**
     * 读数据
     *
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        tel = dataInput.readUTF();
        date = dataInput.readUTF();
    }
}
