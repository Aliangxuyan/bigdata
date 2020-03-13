package com.lxy

import java.util.Properties

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

//先创建一个包装类
class KafkaProxy(brokers:String){

  private val pros:Properties = new Properties()
  pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
  pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

  private val kafkaConn = new KafkaProducer[String,String](pros)

  def send(topic:String, key:String, value:String): Unit ={
    kafkaConn.send(new ProducerRecord[String,String](topic,key,value))
  }

  def send(topic:String,value:String): Unit ={
    kafkaConn.send(new ProducerRecord[String,String](topic,value))
  }

  def close(): Unit ={
    kafkaConn.close()
  }

}

//需要创建一个用于创建对象的工厂
class KafkaProxyFactory(brokers:String) extends BasePooledObjectFactory[KafkaProxy]{
  //创建一个实例
  override def create(): KafkaProxy = new KafkaProxy(brokers)

  override def wrap(t: KafkaProxy): PooledObject[KafkaProxy] = new DefaultPooledObject[KafkaProxy](t)
}



object KafkaPool {

  //声明一个连接池对象
  private var kafkaProxyPool: GenericObjectPool[KafkaProxy] = null

  def apply(brokers:String) : GenericObjectPool[KafkaProxy] = {
    if(null == kafkaProxyPool){
      KafkaPool.synchronized{

        if(null == kafkaProxyPool){
          kafkaProxyPool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(brokers))
        }

      }
    }
    kafkaProxyPool
  }

}
