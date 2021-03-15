package com.lxy.flinksql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

/**
 * @author lxy
 * @date 2020/10/16
 */
object TableApiTest {
  def main(args: Array[String]): Unit = {
    // 1、
    //1.1创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)
    //
    //    // 1.2、基于老版本planner 的流处理
    //    val setting = EnvironmentSettings.newInstance().useOldPlanner().build()
    //
    //    val oldStreamTableEnv = StreamTableEnvironment.create(env, setting)
    //
    //    // 基于老版本的批处理
    //    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    //    val oldBatchEnv = BatchTableEnvironment.create(batchEnv)
    //
    //    // 1.3、基于blink planner 的流处理
    //    val blinkStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    //    val blinkStreamEnv = StreamTableEnvironment.create(env, blinkStreamSettings)
    //
    //    //1.4 基于blink planner 的批处理
    //    val blinkBatchTableStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    //    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchTableStreamSettings)


    // 2、链接外部系统读取数据，注册表
    //2.1 读取文件
    val filepath = "/Users/lxy/Documents/Idea_workspace/bigdata/FlinkTutorial/src/main/resources/sensor.txt"
    tableEnv.connect(new FileSystem().path(filepath))
      .withFormat(new OldCsv())
      .withSchema(new Schema().field("id", DataTypes.STRING()).field("timestamp".trim, DataTypes.BIGINT()).field("temperature".trim, DataTypes.DOUBLE())).createTemporaryTable("inputTable")

    val inputTable = tableEnv.from("inputTable")
    inputTable.toAppendStream[(String, Long, Double)].print()

    env.execute("table Api test")

    val explaination: String = tableEnv.explain(inputTable)
    println(explaination)


  }
}
