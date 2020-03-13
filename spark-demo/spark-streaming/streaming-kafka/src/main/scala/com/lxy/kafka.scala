package com.lxy

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author lxy
  *         2020/3/9
  */
object Kafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kafka").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //创建连接Kafka的参数
    val brokerList = "master01:9092,slave01:9092,slave02:9092"
    val zookeeper = "master01:2181,slave01:2181,slave02:2181"
    val sourceTopic = "source2"
    val targetTopic = "target2"
    val groupid = "consumer001"

    //创建连接kafka的参数
    val kafkaParam = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    //连接上Kafka
    // 创建Kafka的连接需要从ZK中去恢复
    var textKafkaDStream:InputDStream[(String,String)] = null

    //创建连接到ZK，查看是否存在数据保存
    val topicDirs = new ZKGroupTopicDirs(groupid, sourceTopic)

    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    val zkClient = new ZkClient(zookeeper)
    val children = zkClient.countChildren(zkTopicPath)

    //如果有保存，那么从上一个状态恢复
    if(children > 0){

      //最终保存上一次的状态
      var fromOffsets:Map[TopicAndPartition, Long] = Map()

      //获取  Kafka集群的元信息
      val topicList = List(sourceTopic)

      //创建一个连接  SimpleConsumer  low level API
      val getLeaderConsumer = new SimpleConsumer("master01",9092, 100000,10000,"OffsetLookUp")

      //创建一个获取元信息的Request
      val request = new TopicMetadataRequest(topicList,0)

      //获取返回的元信息
      val response = getLeaderConsumer.send(request)

      //解出元信息
      val topicMetadataOption = response.topicsMetadata.headOption

      //partitons 就是包含了每一个分区的主节点所在的主机名称
      val partitons = topicMetadataOption match {
        case Some(tm) => tm.partitionsMetadata.map( pm => (pm.partitionId, pm.leader.get.host)).toMap[Int,String]
        case None => Map[Int,String]()
      }

      getLeaderConsumer.close()
      println("partition info is: " + partitons)
      println("children info is: " + children)


      //获取每一个分区的最小Offset
      for(i <- 0 until children){
        //先获取ZK中第i个分区保存的offset
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")

        println(s"Patition ${i} 目前的Offset是： ${partitionOffset}")

        //获取第i个分区的最小Offset

        //创建一个到第i个分区主分区所在的Host上的连接
        val consumerMin = new SimpleConsumer(partitons(i),9092,100000,10000,"getMinOffset")

        //创建一个请求
        val tp = TopicAndPartition(sourceTopic,i)
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1) ))

        //获取最小的Offset
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets

        consumerMin.close()

        //校准
        var nextOffset = partitionOffset.toLong
        if(curOffsets.length >0 && nextOffset < curOffsets.head){
          nextOffset = curOffsets.head
        }

        fromOffsets += (tp -> nextOffset)
        println(s"Patition ${i} 校准后的Offset是： ${nextOffset}")

      }

      zkClient.close()
      println("从ZK中恢复创建")
      val messageHandler = (mmd: MessageAndMetadata[String,String]) => (mmd.topic, mmd.message())
      textKafkaDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParam,fromOffsets,messageHandler)

    }else{
      //直接创建
      println("直接创建")
      textKafkaDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParam,Set(sourceTopic))
    }

    //注意：需要先拿到新读取进来的数据的Offset，不要转换成为另外一个DStream后再去拿去
    var offsetRanges = Array[OffsetRange]()

    val textKafkaDStream2 = textKafkaDStream.transform{ rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    textKafkaDStream2.map(s => "key:"+ s._1 + "  value:" + s._2).foreachRDD{ rdd =>

      rdd.foreachPartition{ items =>

        //写回Kafka

        //创建到Kafka的连接
        val pool = KafkaPool(brokerList)
        val kafkaProxy = pool.borrowObject()

        //写数据
        for(item <- items)
          kafkaProxy.send(targetTopic, item)

        pool.returnObject(kafkaProxy)
        //关闭连接

      }

      //需要将Kafka每一个分区中读取的Offset更新到ZK
      val updateTopicDirs = new ZKGroupTopicDirs(groupid,sourceTopic)
      val updateZkClient = new ZkClient(zookeeper)

      for(offset <- offsetRanges){
        //将读取的最新的Offset保存到ZK中
        println(s"Patiton ${offset.partition} 保存到ZK中的数据Offset 是：${offset.fromOffset.toString}" )
        val zkPath = s"${updateTopicDirs.consumerOffsetDir}/${offset.partition}"
        ZkUtils.updatePersistentPath(updateZkClient, zkPath, offset.fromOffset.toString)
      }
      updateZkClient.close()

    }


    ssc.start()
    ssc.awaitTermination()
  }

}
