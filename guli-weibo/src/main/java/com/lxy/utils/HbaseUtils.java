package com.lxy.utils;

import com.lxy.constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-12-01
 * <p>
 * 1、创建命名空间
 * 2、判断表是否存在
 * 3、创建表（三张表）
 */
public class HbaseUtils {

    /**
     * 1、创建命名空间
     *
     * @param nameSpace
     */
    public static void createNameSpace(String nameSpace) throws IOException {

        // 1、获取Connnection 对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 2、获取Admin 对象
        Admin admin = connection.getAdmin();

        // 3、构建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        // 4、创建命名空间
        admin.createNamespace(namespaceDescriptor);

        // 5、关闭资源
        admin.close();
        connection.close();
    }

    // 2、判断表是否存在
    private static boolean isTableExist(String tableName) throws IOException {

        // 1、获取Connnection 对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 2、获取Admin 对象
        Admin admin = connection.getAdmin();

        // 3、判断表是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));


        // 4、关闭资源
        admin.close();
        connection.close();

        // 5、返回结果
        return exists;
    }

    /**
     * 3、创建表
     */
    public static void createTable(String tableName, int versions, String... cfs) throws IOException {

        // 1、判断是否传入了列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息");
            return;
        }

        // 2、判断表是否存在
        boolean tableExist = isTableExist(tableName);
        if (tableExist) {
            System.out.println("该表以创建");
        }

        //3、获取connection 对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        // 4、获取Admin对象
        Admin admin = connection.getAdmin();

        // 5、创建表描述起
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        // 6、添加列族信息

        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            // 7、设置版本
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        // 8、创建表操作
        admin.createTable(hTableDescriptor);
        // 9、关闭资源
        admin.close();
        connection.close();

    }
}
