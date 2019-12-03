package com.atguigu.hbase.hbase;

import com.atguigu.hbase.HbaseAPI;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.junit.Test;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-11-28
 * DDL
 * 1、判断表是否存在
 * 2、创建表
 * 3、创建命名空间
 * 4、删除表
 * <p>
 * DML
 * 5、插入数据
 * 6、查数据（scan）
 * 7、查数据（scan）
 * 8、删除表数据
 */
public class TestAPI {
    public static void main(String[] args) {


    }


    @Test
    public void isExistTable() throws IOException {
        boolean stu5 = HbaseAPI.isTableExist("stu6");
        System.out.println("&&&&&&&&&&&&:" + stu5);
    }

    @Test
    public void createTable() {
        try {
            HbaseAPI.createTable("stu6", "info");
            // 在命名空间下创建表
//            HbaseAPI.createTable("hbasedemo1128:stu6", "info");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HbaseAPI.close();
        }
    }

    @Test
    public void dropTable() {
        try {
            HbaseAPI.dropTable("stu6");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HbaseAPI.close();
        }
    }

    /**
     * Caused by: java.lang.IllegalArgumentException: Illegal character <45> at 5. Namespaces can only contain 'alphanumeric characters': i.e. [a-zA-Z_0-9]: hbase-demo-1128
     *
     * @throws IOException
     */
    @Test
    public void createNamespace() throws IOException {
        try {
            HbaseAPI.createNamespace("hbasedemo1128");
        } catch (NamespaceExistException e) {
            System.out.println("该命名空间已存在！！！");
        } finally {
            HbaseAPI.close();
        }
    }

    @Test
    public void addRowData() throws IOException {
        try {
            HbaseAPI.addRowData("stu6", "10001", "info", "age", "12");
        } catch (NamespaceExistException e) {
            System.out.println("该命名空间已存在！！！");
        } finally {
            HbaseAPI.close();
        }
    }

    @Test
    public void getRowQualifier() throws IOException {
        try {
            HbaseAPI.getRowQualifier("stu6", "10001", "info", "name");
        } finally {
            HbaseAPI.close();
        }
    }

    @Test
    public void getRow() throws IOException {
        try {
            HbaseAPI.getRow("stu6", "10001");
        } finally {
            HbaseAPI.close();
        }
    }

    @Test
    public void getAllRows() throws IOException {
        try {
            HbaseAPI.getAllRows("stu6");
        } finally {
            HbaseAPI.close();
        }
    }

    @Test
    public void deleteData() throws IOException {
        try {
            HbaseAPI.deleteData("stu6", "1002", "info", "name");
        } finally {
            HbaseAPI.close();
        }
    }
}

