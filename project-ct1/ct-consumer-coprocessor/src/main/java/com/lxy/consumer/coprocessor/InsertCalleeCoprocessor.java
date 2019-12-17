package com.lxy.consumer.coprocessor;

import com.lxy.common.bean.BaseDao;
import com.lxy.common.constant.Names;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-12-16
 * <p>
 * 使用协处理器 保存被叫用户的数据
 * <p>
 * 1、协处理器的使用
 * 1）创建类
 * 2）让表知道协处理类（和表有关联） tableDescriptor.addCoprocessor(coprocessorClass);
 * 3）
 */
public class InsertCalleeCoprocessor extends BaseRegionObserver {
    // 方法的命名规则
    //login
    //logout
    //prePut
    //doPut 模版方法的设计模式
    //      存在父子类
    //      父类搭建算法的骨架
    //      1、tel 取用户代码，2 时间取年月  3、异或运算     4、hash 散列
    //      子类重写算法的细节
    //      do1,tel取后四位   do2 201810   do3^    do4%&
    //postPut

    /**
     * 保存主叫用户数据之后，由Hbase  自动保存被叫用户数据
     *
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        // 获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf(Names.TABLE.value()));

        String rowKey = Bytes.toString(put.getRow());

        //0_13333333333_20180702092528_17777777777_0063_1
        String[] splits = rowKey.split("_");

        // 被叫用户 可以这样通过列族些，但是不是最优选择
        String call1 = splits[1];
        String call2 = splits[3];
        String calltime = splits[2];
        String duration = splits[4];
        String flag = splits[5];

        // 只有主叫用户保存后才需要出发被叫用户的保存，不然后面的put  会形成死循环
        if ("0".equals(flag)) {
            return;
        }


        CoprocessorDao dao = new CoprocessorDao();
        String calleeRowkey = dao.getReginNum(call2, calltime) + "_" + call1 + "_" + calltime + "_"
                + call2 + "_" + duration + "_0";


        Put calleePut = new Put(Bytes.toBytes(calleeRowkey));
        byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
        calleePut.addColumn(calleeFamily, Bytes.toBytes("call1"), Bytes.toBytes(call2));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("call2"), Bytes.toBytes(call1));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("duration"), Bytes.toBytes(duration));
        calleePut.addColumn(calleeFamily, Bytes.toBytes("flag"), Bytes.toBytes("0"));
        //保存数据 执行完put 之后执行协处理postPut，不处理会形成死循环
        table.put(calleePut);
        // 关闭，防止内存溢出
        table.close();
    }

    private class CoprocessorDao extends BaseDao {
        public int getReginNum(String tel, String time) {
            return genReginsNum(tel, time);

        }

    }
}
