package com.lxy.producer.bean;

import com.lxy.common.bean.DataIN;
import com.lxy.common.bean.DataOut;
import com.lxy.common.bean.Producer;
import com.lxy.common.utils.DateUtil;
import com.lxy.common.utils.NumberUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @author lxy
 * @date 2019-11-18
 * b本地数据文件的生产者
 */
public class LocalFileProducer implements Producer {
    private DataIN in;
    private DataOut out;
    private volatile boolean flag;

    public void setIn(DataIN in) {
        this.in = in;
    }

    public void setOut(DataOut out) {
        this.out = out;
    }

    // 生产数据
    public void producer() {

        flag = true;
        // 读取通讯录的数据

        try {
            List<Contact> contacts = in.read(Contact.class);
            while (flag) {
                //从通讯里中随机查找两个电话好吗（主叫，被叫）
                int call1Index = new Random().nextInt(contacts.size());
                int call2Index;
                while (true) {
                    call2Index = new Random().nextInt(contacts.size());
                    if (call1Index != call2Index) {
                        break;
                    }
                }
                //生成随机的通话时间
                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);

                //生成随机的通话时长
                String duration = NumberUtil.format(new Random().nextInt(3000), 4);

                String startDate = "20180101000000";
                String endDate = "20190101000000";

                long startTime = DateUtil.parse(startDate, "yyyyMMddHHmmss").getTime();
                long endTime = DateUtil.parse(endDate, "yyyyMMddHHmmss").getTime();

                long callTime = startTime + (long) ((endTime - startTime) * Math.random());
                String callTimeStr = DateUtil.format(new Date(callTime), "yyyyMMddHHmmss");

                //生成通话记录
                CallLog log = new CallLog(call1.getTel(), call2.getTel(), callTimeStr, duration);
                System.out.println(log);
                //将通话记录刷写到数据文件中
                out.write(log);
                Thread.sleep(500);


            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭生产者
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
        if (out != null) {
            out.close();
        }
    }
}
