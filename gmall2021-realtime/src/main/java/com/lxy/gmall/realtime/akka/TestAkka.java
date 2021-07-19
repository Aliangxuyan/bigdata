package com.lxy.gmall.realtime.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

/**
 * @author lxy
 * @date 2021/7/19
 *
 * 使用Akka
 */
public class TestAkka {
    public static void main(String[] args) {
        // 1. 构建ActorSystem
        // 使用缺省配置
        ActorSystem system = ActorSystem.create("sys");
        // 也可显示指定appsys配置
        // ActorSystem system1 = ActorSystem.create("helloakka", ConfigFactory.load("appsys"));
        // 2. 构建Actor,获取该Actor的引用，即ActorRef
        ActorRef helloActor = system.actorOf(Props.create(HelloActor.class), "helloActor");
        // 3. 给helloActor发送消息
        helloActor.tell("hello helloActor", ActorRef.noSender());
        // 4. 关闭ActorSystem
        system.terminate();
    }
}
