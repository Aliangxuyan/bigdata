package com.lxy.ct.cache;

import com.lxy.common.utils.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author lxy
 * @date 2019-12-16
 * 启动缓存客户端，想redis 中增加缓存数据
 */
public class Bootstrap {
    public static void main(String[] args) {
        Connection connection = null;
        Map<String, Integer> userMap = new HashMap<String, Integer>();
        Map<String, Integer> dataMap = new HashMap<String, Integer>();

        //读取mysql  中的数据

        //像redis 中存储数据
//读取用户，时间数据
        String queryUserSql = "select id,tel from ct_user";
        String queryDateSql = "select id,year,month,day from ct_date";
        PreparedStatement psstat = null;
        ResultSet rs = null;
        try {
            connection = JDBCUtil.getConnection();
            psstat = connection.prepareStatement(queryUserSql);
            rs = psstat.executeQuery();

            while (rs.next()) {
                Integer id = rs.getInt(1);
                String tel = rs.getString(2);
                userMap.put(tel, id);
            }

            psstat = connection.prepareStatement(queryDateSql);
            rs = psstat.executeQuery();

            while (rs.next()) {
                Integer id = rs.getInt(1);
                String year = rs.getString(2);
                String month = rs.getString(3);
                String day = rs.getString(4);
                if (month.length() == 1) {
                    month = "0" + month;
                }
                if (day.length() == 1) {
                    day = "0" + day;
                }
                dataMap.put(year + month + day, id);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (psstat != null) {
                try {
                    psstat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }

//        System.out.println(userMap.size());
//        System.out.println(dataMap.size());

        Jedis jedis = new Jedis("localhost", 6379);
        Iterator<String> keyIterator = userMap.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            Integer value = userMap.get(key);
            jedis.hset("ct_user", key, value + "");
        }

        keyIterator = dataMap.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            Integer value = dataMap.get(key);
            jedis.hset("ct_date", key, value + "");
        }


    }
}
