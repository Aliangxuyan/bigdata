package com.lxy.common.bean;

import com.lxy.common.api.Column;
import com.lxy.common.api.Rowkey;
import com.lxy.common.api.TableRef;
import com.lxy.common.constant.Names;
import com.lxy.common.constant.ValueConstant;
import com.lxy.common.utils.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

/**
 * @author lxy
 * @date 2019-12-15
 * <p>
 * 基础的数据访问对象
 */
public abstract class BaseDao {

    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();


    protected void start() throws Exception {
        getConnection();
    }

    protected void end() throws Exception {
        Admin admin = getAdmin();
        if (admin != null) {
            admin.close();
            adminHolder.remove();
        }
        Connection conn = getConnection();
        if (conn != null) {
            conn.close();
            connHolder.remove();
        }

    }

    /**
     *
     * 测试
     * 5_17801042256_2018-08~~~~5_17801042256_2018-08|
     * 5_17801042256_2018-09~~~~5_17801042256_2018-09|
     * 4_17801042256_2018-10~~~~4_17801042256_2018-10|
     * 4_17801042256_2018-11~~~~4_17801042256_2018-11|
     * 4_17801042256_2018-12~~~~4_17801042256_2018-12|
     * 4_17801042256_2019-01~~~~4_17801042256_2019-01|
     * 4_17801042256_2019-02~~~~4_17801042256_2019-02|
     * 4_17801042256_2019-03~~~~4_17801042256_2019-03|
     * 4_17801042256_2019-04~~~~4_17801042256_2019-04|
     * 4_17801042256_2019-05~~~~4_17801042256_2019-05|
     * 4_17801042256_2019-06~~~~4_17801042256_2019-06|
     *
     *  public static void main(String[] args) {
     *         List<String[]> strings = getStartStoreRowkey("17801042256", "201808", "201906");
     *         for (String[] string : strings) {
     *             System.out.println(string[0] + "~~~~" + string[1]);
     *         }
     *     }
     */

    /**
     * 获取查询时 的startrow ,stoprow 集合
     * 需求：根据分区号，查询范围数据
     *
     * @param tel
     * @param start
     * @param end
     * @return
     */
    protected List<String[]> getStartStoreRowkey(String tel, String start, String end) {
        List<String[]> rowKeyss = new ArrayList<String[]>();

        String startTime = start.substring(0, 6);
        String endTime = end.substring(0, 6);

        Calendar startCal = Calendar.getInstance();
        startCal.setTime(DateUtil.parse(startTime, "yyyyMM"));

        Calendar endCal = Calendar.getInstance();
        endCal.setTime(DateUtil.parse(endTime, "yyyyMM"));


        while (startCal.getTimeInMillis() <= endCal.getTimeInMillis()) {
            // 当前时间
            String nowTime = DateUtil.format(startCal.getTime(), "yyyy-MM");

            int regionNum = genReginsNum(tel, nowTime);

            // 1_133_201803 ~ 1_133_20183|
            String startRow = regionNum + "_" + tel + "_" + nowTime;
            String stopRow = startRow + "|";

            String[] rowKeys = {startRow, stopRow};
            rowKeyss.add(rowKeys);
            // 月份加1
            startCal.add(Calendar.MONTH, 1);
        }
        return rowKeyss;
    }

    /**
     * 计算分区号，因为时统计一个用户指定时间的通话时长信息所以分区中要时间和用户信息
     *
     * @param tel
     * @param date
     * @return
     */
    protected static int genReginsNum(String tel, String date) {

        // 获取手机号无规律的部分，用户编码
        String userCode = tel.substring(tel.length() - 4);
        String yearMonth = date.substring(0, 6);

        int userCodeHash = userCode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        //crc 校验采用异或算法
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        // 取模
        int regioNum = crc % ValueConstant.REGION_COUNT;
        return regioNum;
    }


    /**
     * 生成分区健
     *
     * @param regionCount
     * @return
     */
    protected byte[][] genSplitKeys(Integer regionCount) {
        // 例如6哥分区时为5个分区健
        // 0,1,2,3,
        // (,0),[0,1][1,]
        // (,0|),[0|,1|][1|,] 竖线大于其他数字值
        int splitKeyCount = regionCount - 1;
        List<byte[]> bslist = new ArrayList<byte[]>();
        for (int i = 0; i < regionCount; i++) {
            String splitKey = i + "|";
            bslist.add(Bytes.toBytes(splitKey));
        }
        byte[][] bs = new byte[splitKeyCount][];

        // 也可以按照分区排序，但是这块的规则生成的就是有序的，所以可以不配置
        Collections.sort(bslist, new Bytes.ByteArrayComparator());

        return bslist.toArray(bs);
    }

    /**
     * 增加对象，自动封装数据，将对象直接保存到HBase 中
     *
     * @param obj
     * @throws Exception
     */
    protected void putData(Object obj) throws Exception {
        // 反射
        Class<?> clazz = obj.getClass();
        TableRef tableRef = clazz.getAnnotation(TableRef.class);
        Field[] fs = clazz.getDeclaredFields();

        String rowkeyStr = "";
        for (Field f : fs) {
            Rowkey rowKey = f.getAnnotation(Rowkey.class);
            if (rowKey != null) {
                f.setAccessible(true);
                rowkeyStr = (String) f.get(obj);
                break;
            }
        }
        // 注解中的表名
        String name = tableRef.value();
        Connection conn = getConnection();
        TableName tableName = TableName.valueOf(name);
        Table table = conn.getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkeyStr));

        for (Field f : fs) {
            Column column = f.getAnnotation(Column.class);
            if (column != null) {
                f.setAccessible(true);
                String family = column.family();
                String colName = column.column();
                if (colName == null || "".equals(colName)) {
                    colName = f.getName();
                }
                f.setAccessible(true);
                String value = (String) f.get(obj);
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(colName), Bytes.toBytes(value));
            }
        }
        // 增加数据
        table.put(put);
        // 关闭表
        table.close();

    }

    /**
     * 增加多条数据
     *
     * @param name
     * @param puts
     * @throws Exception
     */
    protected void putData(String name, List<Put> puts) throws Exception {
        // 获取表对象
        Connection conn = getConnection();
        TableName tableName = TableName.valueOf(name);
        Table table = conn.getTable(tableName);

        // 增加数据
        table.put(puts);

        // 关闭表
        end();
    }


    /**
     * 写入Hbase
     *
     * @param name
     * @param put
     * @throws Exception
     */
    protected void putData(String name, Put put) throws Exception {
        // 获取表对象
        Connection conn = getConnection();
        TableName tableName = TableName.valueOf(name);
        Table table = conn.getTable(tableName);

        // 增加数据
        table.put(put);

        // 关闭表
        end();
    }

    /**
     * 创建表，如果有删除，没有创建,可能是多个列族
     */
    protected void createTableXX(String name, String coprocessorClass, Integer regionCount, String... families) throws Exception {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        boolean exists = admin.tableExists(tableName);
        if (exists) {
            // 表存在，删除表
            deleteTable(name);
        }
        // 创建表
        createTable(name, coprocessorClass, regionCount, families);
    }

    protected void createTable(String name, String coprocessorClass, String... families) throws Exception {
        createTable(name, coprocessorClass, null, families);
    }

    /**
     * 真正的创建表
     *
     * @param name
     * @throws Exception
     */
    protected void createTable(String name, String coprocessorClass, Integer regionCount, String... families) throws Exception {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

        if (families == null || families.length == 0) {
            families = new String[1];
            families[0] = Names.CF_INFO.getValue();
        }

        // 创建列族
        for (String family : families) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(hColumnDescriptor);
        }

        // 让表知道协处理类（和表有关联）
        if (StringUtils.isNotEmpty(coprocessorClass)) {
            tableDescriptor.addCoprocessor(coprocessorClass);
        }

        // 增加预分区
        // 分区键
        if (regionCount == null || regionCount <= 0) {
            admin.createTable(tableDescriptor);
        } else {
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(tableDescriptor, splitKeys);
        }
    }


    protected void deleteTable(String name) throws Exception {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * 创建命名空间，存在不需要创建，否则创建
     *
     * @param namespace
     */
    protected void createNameSpaceNx(String namespace) throws Exception {
        Admin admin = getAdmin();
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
            NamespaceDescriptor namespaceDescriptor =
                    NamespaceDescriptor.create(namespace).build();

            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 获取admin 对象
     *
     * @return
     * @throws Exception
     */
    protected synchronized Admin getAdmin() throws Exception {
        Admin admin = adminHolder.get();
        if (admin == null) {
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    /**
     * 获取连接对象
     *
     * @return
     * @throws Exception
     */
    protected synchronized Connection getConnection() throws Exception {
        Connection conn = connHolder.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
        return conn;
    }
}
