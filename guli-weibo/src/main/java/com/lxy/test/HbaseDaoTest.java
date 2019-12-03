package com.lxy.test;

import com.lxy.constants.Constants;
import com.lxy.dao.HbaseDao;
import com.lxy.utils.HbaseUtils;

import java.io.IOException;

/**
 * @author lxy
 * @date 2019-12-01
 */
public class HbaseDaoTest {

    public static void init() {
        // 创建命名空间
        try {
            HbaseUtils.createNameSpace(Constants.NAMESPACE);
            //创建微博内容表
            HbaseUtils.createTable(Constants.CONTENT_TABLE, Constants.CONTENT_TABLE_VERSIONs, Constants.CONTENT_TABLE_CF);

            //创建用户关系表
            HbaseUtils.createTable(Constants.RALATION_TABLE, Constants.RALATION_TABLE_VERSIONs, Constants.RALATION_TABLE_CF1, Constants.RALATION_TABLE_CF2);

            //创建收件箱表
            HbaseUtils.createTable(Constants.INBOX_TABLE, Constants.INBOX_TABLE_VERSIONs, Constants.INBOX_TABLE_CF);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        //初始化
        init();

        //1001 发布微博
        HbaseDao.publishWeb("1001", "赶紧下课吧");

        //1002关注1001和1003
        HbaseDao.addAttents("1002", "1001", "1003");

        //获取1002初始化页面
        HbaseDao.getInit("1002");
        System.out.println("&&&&&&&&&&&&&&&&&&&&111&&&&&&&&&&&&&&&&&&&&&&&");

        //1003 发布3调微博，同时1001 发布2条微博
        HbaseDao.publishWeb("1003", "谁说赶紧下课吧");
        Thread.sleep(10);
        HbaseDao.publishWeb("1001", "我没说话");
        Thread.sleep(10);
        HbaseDao.publishWeb("1003", "那谁说的");
        Thread.sleep(10);
        HbaseDao.publishWeb("1001", "我没说话");
        Thread.sleep(10);
        HbaseDao.publishWeb("1003", "谁说赶紧下课吧11111");
        Thread.sleep(10);

        //获取1002 的初始化页面
        HbaseDao.getInit("1002");
        System.out.println("&&&&&&&&&&&&&&&&&&&&2222&&&&&&&&&&&&&&&&&&&&&&&");

        //1002取关1003
        HbaseDao.deleteAttends("1002", "1003");

        //1002再次关注1003
        HbaseDao.addAttents("1002", "1003");

        //获取1002 初始化页面
        HbaseDao.getInit("1002");

        // 获取1001 详情信息
        HbaseDao.getWeiBoDetail("1001");
    }
}
