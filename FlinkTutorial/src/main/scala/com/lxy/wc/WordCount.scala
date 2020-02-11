package com.lxy.wc

import org.apache.flink.api.scala._

/**
  * @author lxy
  * @date 2020-01-19
  */
// 批处理wordCount 程序
object WordCount {
  def main(args: Array[String]): Unit = {

    // 创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 可以从文件中读取数据
    val inputPath = "/Users/lxy/Documents/Idea_workspace/bigdata/FlinkTutorial/src/main/resources/hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    // 切分数据得到 word, 燃火再按word 做分组聚合
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()
  }
}
