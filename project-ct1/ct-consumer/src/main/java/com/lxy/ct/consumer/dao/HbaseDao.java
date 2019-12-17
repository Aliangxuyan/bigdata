package com.lxy.ct.consumer.dao;

import com.lxy.common.bean.BaseDao;
import com.lxy.common.constant.Names;
import com.lxy.common.constant.ValueConstant;
import com.lxy.ct.consumer.bean.Calllog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lxy
 * @date 2019-12-15
 */
public class HbaseDao extends BaseDao {

    /**
     * 初始化
     */
    public void init() throws Exception {
        start();
        createNameSpaceNx(Names.NAMESPACE.getValue());
        createTableXX(Names.TABLE.getValue(), "com.lxy.consumer.coprocessor.InsertCalleeCoprocessor", ValueConstant.REGION_COUNT, Names.CF_CALLER.getValue(), Names.CF_CALLEE.getValue());
        end();
    }

    /**
     * 直接将对象写入到Hbase,避免重复代码
     *
     * @param calllog
     * @throws Exception
     */
    public void insertData(Calllog calllog) throws Exception {
        calllog.setRowkey(genReginsNum(calllog.getCall1(), calllog.getCalltime()) + "_" + calllog.getCall1() + "_" + calllog.getCalltime() + "_"
                + calllog.getCall2() + "_" + calllog.getDuration());
        putData(calllog);
    }

    /**
     * 插入数据
     *
     * @param value
     */
    public void inserData(String value) throws Exception {
        // 将通话日志保存到Hbase

        // 1、获取通话日志数据
        String[] values = value.split("\t");
        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];

        // 2、创建数据对象

        //rowkey 的设计
        //1）长度原则
        //       最大长度64k，推荐长度为10~100kb
        //       最好时8的背书，能短则短，rowKey 太长会影响性能
        //2）唯一原则，rowKey 应该具有唯一性
        //3）散列原则
        //      3-1）盐值散列，不能使用时间戳直接作为rowkey
        //          在rowkey 钱增加随机数
        //      3-2）字符串反转，时间戳和手机号
        //      3-3）计算分区好：hashMap

        // rowkey = regionNum + call1 + time + call2 +duration
        String rowkey = genReginsNum(call1, calltime) + "_" + call1 + "_" + calltime + "_"
                + call2 + "_" + duration + "_1";
        System.out.println("rowKey: " + rowkey);
        try {
            Put put = new Put(Bytes.toBytes(rowkey));
            byte[] family = Bytes.toBytes(Names.CF_CALLER.getValue());
            // 主叫用户
            put.addColumn(family, Bytes.toBytes("call1"), Bytes.toBytes(call1));
            put.addColumn(family, Bytes.toBytes("call2"), Bytes.toBytes(call2));
            put.addColumn(family, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
            put.addColumn(family, Bytes.toBytes("duration"), Bytes.toBytes(duration));
            put.addColumn(family, Bytes.toBytes("flag"), Bytes.toBytes("1"));


//            String calleeRowkey = genReginsNum(call2, calltime) + "_" + call2 + "_" + calltime + "_"
//                    + call1 + "_" + duration + "_0";
//            // 被叫用户 可以这样通过列族些，但是不是最优选择
//            Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
//            byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
//            calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
//            calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
//            calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
//            calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
//            calleePut.addColumn(calleeFamily, Bytes.toBytes("flag"), Bytes.toBytes("0"));

            List<Put> puts = new ArrayList<Put>();
            puts.add(put);
//            puts.add(calleePut);
            // 保存数据
            putData(Names.TABLE.getValue(), puts);
        } catch (IllegalArgumentException e) {
            return;
        }
    }
}
