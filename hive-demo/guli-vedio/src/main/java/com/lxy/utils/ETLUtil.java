package com.lxy.utils;

/**
 *
 */
public class ETLUtil {

    public static String oriString2ETLString(String ori) {

        // 0.切割数据
        String[] fields = ori.split("\t");

        // 1.过滤脏数据（不符合要求的数据）
        if (fields.length < 9) {
            return null;
        }

        // 2.将类别字段中的" " 替换为""（即去掉类别字段中的空格）
        fields[3] = fields[3].replace(" ", "");

        // 3.替换关联视频字段分隔符"\t"替换为"&"
        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < fields.length; i++) {
            // 关联视频字段之前的数据
            if (i < 9) {
                if (i == fields.length -1) {
                    sb.append(fields[i]);
                } else {
                    sb.append(fields[i] + "\t");
                }
            } else {
                // 关联视频字段的数据
                if (i == fields.length -1) {
                    sb.append(fields[i]);
                } else {
                    sb.append(fields[i] + "&");
                }
            }
        }
        // 得到的数据格式为：bqZauhidT1w	bungloid	592	Film&Animation	28	374550	4.19	3588	1763	QJ5mXzC1YbQ&geEBYTZ4EB8
        return sb.toString();
    }
}

