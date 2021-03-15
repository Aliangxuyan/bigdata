package com.lxy.gmall.realtime.app.func;

import com.lxy.gmall.realtime.bean.TableProcess;
import com.lxy.gmall.realtime.common.GmallConfig;
import com.lxy.gmall.realtime.utils.MySQLUtil;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author lxy
 * @date 2021/3/15
 * 用于对业务数据进行分流处理的自定义处理函数
 */
public class TableProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    //因为要将维度数据写到侧输出流，所以定义一个侧输出流标签
    private OutputTag<JSONObject> outputTag;

    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //用于在内存中存储表配置对象 [表名,表配置信息]
    private Map<String, TableProcess> tableProcessMap = new HashMap<>();

    //表示目前内存中已经存在的HBase表
    private Set<String> existsTables = new HashSet<>();

    //声明Phoenix连接
    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化Phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //初始化配置表信息
        initTableProcessMap();
        //开启定时任务,用于不断读取配置表信息  从现在起过delay毫秒以后，每隔period更新一次
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                initTableProcessMap();
            }
        }, 5000, 5000);
    }

    //读取MySQL中配置表信息，存入到内存Map中
    private void initTableProcessMap() {
        System.out.println("更新配置的处理信息");
        //查询MySQL中的配置表数据
        List<TableProcess> tableProcessList = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
        //遍历查询结果,将数据存入结果集合
        for (TableProcess tableProcess : tableProcessList) {
            //获取源表表名
            String sourceTable = tableProcess.getSourceTable();
            //获取数据操作类型
            String operateType = tableProcess.getOperateType();
            //获取结果表表名
            String sinkTable = tableProcess.getSinkTable();
            //获取sink类型
            String sinkType = tableProcess.getSinkType();
            //拼接字段创建主键
            String key = sourceTable + ":" + operateType;
            //将数据存入结果集合
            tableProcessMap.put(key, tableProcess);
            //如果是向Hbase中保存的表，那么判断在内存中维护的Set集合中是否存在
            if ("insert".equals(operateType) && "hbase".equals(sinkType)) {
                boolean notExist = existsTables.add(sourceTable);
                //如果表信息数据不存在内存,则在Phoenix中创建新的表
                if (notExist) {
                    checkTable(sinkTable, tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
                }
            }
        }
        if (tableProcessMap==null || tableProcessMap.size() == 0) {
            throw new RuntimeException("缺少处理信息");
        }
    }
    private void checkTable(String tableName, String fields, String pk, String ext) {
        //主键不存在,则给定默认值
        if (pk == null) {
            pk = "id";
        }
        //扩展字段不存在,则给定默认值
        if (ext == null) {
            ext = "";
        }
        //创建字符串拼接对象,用于拼接建表语句SQL
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        //将列做切分,并拼接至建表语句SQL中
        String[] fieldsArr = fields.split(",");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];
            if (pk.equals(field)) {
                createSql.append(field).append(" varchar primary key ");
            } else {
                createSql.append("info.").append(field).append(" varchar");
            }
            if (i < fieldsArr.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")");
        createSql.append(ext);

        try {
            //执行建表语句在Phoenix中创建表
            System.out.println(createSql);
            PreparedStatement ps = connection.prepareStatement(createSql.toString());
            ps.execute();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败！！！");
        }
    }
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] cols = StringUtils.split(sinkColumns, ",");
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        List<String> columnList = Arrays.asList(cols);
        for (Iterator<Map.Entry<String, Object>> iterator = entries.iterator(); iterator.hasNext(); ) {
            Map.Entry<String, Object> entry = iterator.next();
            if (!columnList.contains(entry.getKey())) {
                iterator.remove();
            }
        }
    }
    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        //如果是使用Maxwell的初始化功能，那么type类型为bootstrap-insert,我们这里也标记为insert，方便后续处理
        if (type.equals("bootstrap-insert")) {
            type = "insert";
            jsonObj.put("type", type);
        }

        //获取配置表的信息
        if (tableProcessMap != null && tableProcessMap.size() > 0) {
            String key = table + ":" + type;
            TableProcess tableProcess = tableProcessMap.get(key);
            if (tableProcess != null) {
                jsonObj.put("sink_table", tableProcess.getSinkTable());
                if (tableProcess.getSinkColumns() != null && tableProcess.getSinkColumns().length() > 0) {
                    filterColumn(jsonObj.getJSONObject("data"), tableProcess.getSinkColumns());
                }
            } else {
                System.out.println("No This Key:" + key);
            }
            if (tableProcess != null && TableProcess.SINK_TYPE_HBASE.equalsIgnoreCase(tableProcess.getSinkType())) {
                ctx.output(outputTag, jsonObj);
            } else if (tableProcess != null && TableProcess.SINK_TYPE_KAFKA.equalsIgnoreCase(tableProcess.getSinkType())) {
                out.collect(jsonObj);
            }
        }
    }
}
