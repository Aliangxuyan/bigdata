package com.lxy


import com.lxy.apitest.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * @author lxy
  *         2020-01-21
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置全局的并行度
    env.setParallelism(1)

    // 读入数据
    val inputStream = env.readTextFile("/Users/lxy/Documents/Idea_workspace/bigdata/FlinkTutorial/src/main/resources/sensor.txt")

    // 1、基本转换算子和聚合算子**************************************************************************

    // Transform操作
    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        }
      )
    //  def keyBy(fields: Int*): KeyedStream[T, JavaTuple] = asScalaStream(stream.keyBy(fields: _*))
    // 返回是 JavaTuple
    val stream0 = dataStream
      .keyBy(0)
      .sum(2)
    //    stream0.print("stream0")

    //  def keyBy[K: TypeInformation](fun: T => K): KeyedStream[T, K] = {
    val stream11 = dataStream
      .keyBy(_.id)
      .sum(2)
    //    stream11.print("stream11")


    // 1. 聚合操作
    val stream1 = dataStream
      .keyBy("id")
      //            .sum("temperature")
      // 输出当前传感器最新温度+10 ，而时间是上一次时间加1
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))

    stream1.print("stream1")
    // 基本转换算子和聚合算子**************************************************************************

    //2、 多流转换算子，先通过split 转换为 splitStream 的两部分再通过 select  真正意义上的分开**************************************************************************

    // 2. 分流，根据温度是否大于30度划分
    val splitStream = dataStream
      .split(sensorData => {
        if (sensorData.temperature > 30) Seq("high") else Seq("low")
      })

    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low")

    // 3. 合并两条流 先connect 再 coMap    connect 将两个 DataStream 合并到一起，coMap 是真实的合并到一起，可以是不同的流
    val warningStream = highTempStream.map(sensorData => (sensorData.id, sensorData.temperature))

    //def connect[T2](dataStream: DataStream[T2]): ConnectedStreams[T, T2] =  asScalaStream(stream.connect(dataStream.javaStream))
    // def map[R: TypeInformation](fun1: IN1 => R, fun2: IN2 => R): DataStream[R]  转换为一个DataStream
    /**
      * union  和 connect 区别
      *
      * union：可以合并多个，但是数据结构必须一样，（def union(dataStreams: DataStream[T]*): DataStream[T]）
      * connect： 必须转换成 DataStream 只能一次合并两条流，数据结构可以不一样 （def connect[T2](dataStream: DataStream[T2]): ConnectedStreams[T, T2]）
      */
    val connectedStreams = warningStream.connect(lowTempStream) // 里面是两种类型 ConnectedStreams[T, T2]   用map()
    val coMapStream = connectedStreams.map(
      warningData => (warningData._1, warningData._2, "high temperature warning"),
      lowData => (lowData.id, "healthy")
    )
    coMapStream.print("connect")

    val unionStream = highTempStream.union(lowTempStream)
    unionStream.print("union")

    // 函数类
    dataStream.filter(data => data.id.contains("sensor_1")).print() // 匿名函数实现
    dataStream.filter(_.id.contains("sensor_1")).print() // 普通实现
    dataStream.filter(new MyFilter()).print() // 函数类实现，三种方式都可以

    // 输出数据
    //    dataStream.print()
    //    highTempStream.print("high")
    //    lowTempStream.print("low")
    //    allTempStream.print("all")
    //    unionStream.print("union")

    env.execute("transform test job")
  }
}

// 专门类实现，更加通用，可以外面灵活穿值 sensor_1
class MyFilter() extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}

// 富函数，其实不是函数是函数类，拥有更强大的功能，可以获取运行环境上下文、可以拥有一些生命周期方法
class MyMapper() extends RichMapFunction[SensorReading, String] {
  override def map(value: SensorReading): String = {
    "flink"
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)
}