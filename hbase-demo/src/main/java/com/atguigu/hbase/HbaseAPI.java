package com.atguigu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lxy on 2018/8/21.
 */
public class HbaseAPI {
    public static Configuration conf;
    private static Connection connection;
    private static Admin admin;

    static {
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.4");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            // 获取连接
            connection = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        //在HBase中管理、访问表需要先创建HBaseAdmin对象
//Connection connection = ConnectionFactory.createConnection(conf);
//HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        //*****************************************
//        HBaseAdmin admin = new HBaseAdmin(conf);
//        return admin.tableExists(tableName);
        // *******************************
        try {
            boolean exists = admin.tableExists(TableName.valueOf(tableName));
            return exists;
        } finally {
            connection.close();
            admin.close();
        }
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param columnFamily
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     * @throws IOException
     */
    public static void createTable(String tableName, String... columnFamily) throws
            MasterNotRunningException, ZooKeeperConnectionException, IOException {
//        HBaseAdmin admin = new HBaseAdmin(conf);
        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
        } else {
            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功！");
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     * @throws IOException
     */
    public static void dropTable(String tableName) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (isTableExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }
    }


    /**
     * 删除一条数据
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     */
    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        delete.addColumn(cf.getBytes(),cn.getBytes());
        table.delete(delete);
        table.close();

    }

    /**
     * 像表中插入数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName, String rowKey, String columnFamily, String
            column, String value) throws IOException {
        //创建HTable对象
//        HTable hTable = new HTable(conf, tableName);
        Table hTable = connection.getTable(TableName.valueOf(tableName));
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向Put对象中组装数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }

    /**
     * 删除多行
     *
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
//        HTable hTable = new HTable(conf, tableName);
        Table hTable = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
    }

    /**
     * 获取所有的数据
     *
     * @param tableName
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException {
//        HTable hTable = new HTable(conf, tableName);
        Table hTable = connection.getTable(TableName.valueOf(tableName));
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * 获取某一行数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName, String rowKey) throws IOException {
//        HTable table = new HTable(conf, tableName);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

    /**
     * 获取某一行指定“列族:列”的数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public static void getRowQualifier(String tableName, String rowKey, String family, String
            qualifier) throws IOException {
//        HTable table = new HTable(conf, tableName);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }
}
