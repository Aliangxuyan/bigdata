package com.lxy.flinksql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
 * @author lxy
 * @date 2020/11/17
 */
class TestJoin {
//  def main(args: Array[String]): Unit = {
//    // create blink table enviroment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val settings = EnvironmentSettings
//      .newInstance
//      .useBlinkPlanner
//      .inStreamingMode
//      .build
//
//    val tableEnv = StreamTableEnvironment.create(env, settings)
//
//    // get sql，读取sql 文件，按 ; 切成不同的段
//    val sqlList = SqlFileReadUtil.sqlFileReadUtil(sqlName)
//
//    // change special sql name to loop
//    for (sql <- sqlList) {
////      logger.info("sql : {}", sql)
//      tableEnv.sqlUpdate(sql)
//    }
//
//    tableEnv.execute(sqlName)
//  }
}
