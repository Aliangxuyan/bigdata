package com.lxy.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lxy
 * @date 2019-12-29
 */
public class EventJsonUDTF extends GenericUDTF {
    //该方法中，我们将指定输出参数的名称和参数类型： 

    /**
     * @deprecated
     */
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        // 获取输入数据
        String input = objects[0].toString();
        if (StringUtils.isBlank(input)) {
            return;
        } else {
            try {
                JSONArray ja = new JSONArray(input);

                if (ja == null) {
                    return;
                } else {
                    for (int i = 0; i < ja.length(); i++) {
                        String[] result = new String[2];
                        try {
                            result[0] = ja.getJSONObject(i).getString("en");
                            result[0] = ja.getString(i);
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                        forward(result);
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }

        }


    }

    @Override
    public void close() throws HiveException {

    }
}
