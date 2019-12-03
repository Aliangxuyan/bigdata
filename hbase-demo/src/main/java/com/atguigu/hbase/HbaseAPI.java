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
        conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
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
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;

    }

    /**
     * 关闭资源
     */
    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
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

        // 判断列族信息
        if (columnFamily.length <= 0) {
            System.out.println("请设置列族信息");
            return;
        }
        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
            return;
        }

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

    /**
     * 创建命名空间
     *
     * @param namespace
     */
    public static void createNamespace(String namespace) throws IOException {
        NamespaceDescriptor.Builder namespaceDescriptor = NamespaceDescriptor.create(namespace);
        admin.createNamespace(namespaceDescriptor.build());
        System.out.println("创建命名空间成功！");
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
//        HBaseAdmin admin = new HBaseAdmin(conf);
        if (isTableExist(tableName)) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }
    }


    /**
     * 删除一条数据
     *
     *
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     */
    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        delete.addColumn(cf.getBytes(), cn.getBytes());
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
        //  1、创建HTable对象
//        HTable hTable = new HTable(conf, tableName);
        Table hTable = connection.getTable(TableName.valueOf(tableName));
        //  2、创建put 对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //  3、向Put对象中组装数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        // 4、插入数据
        hTable.put(put);
        // 5、关闭表连接，不能直接放到close 里面因为，每次操作的表都不一定是同一个
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
     * scan 扫描获取所有的数据
     *
     * @param tableName
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException {
//        HTable hTable = new HTable(conf, tableName);

        // 1、获取表对象
        Table hTable = connection.getTable(TableName.valueOf(tableName));
        // 2、得到用于扫描region的对象
        Scan scan = new Scan();
        //3、扫描表————使用HTable得到resultcanner实现类的对象
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
        // 1、获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 2、创建get 对象
        Get get = new Get(Bytes.toBytes(rowKey));

        // 设置货物数据的列族和列信息
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

        // 设置获取数据的版本数
        get.setMaxVersions(2);

        //3、获取数据
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }
}
