package com.lxy.gmall.realtime.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.util.Timeout;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * @author lxy
 * @date 2021/7/19
 */
public class TestAkka02 {
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("sys");
        ActorSelection as = system.actorSelection("/path/to/actor");
        Timeout timeout = new Timeout(Duration.create(2, "seconds"));
        Future<ActorRef> fu = as.resolveOne(timeout);
        fu.onSuccess(new OnSuccess<ActorRef>() {
            @Override
            public void onSuccess(ActorRef actor) {
                System.out.println("actor:" + actor);
                actor.tell("hello actor", ActorRef.noSender());
            }
        }, system.dispatcher());
        fu.onFailure(new OnFailure() {
            @Override
            public void onFailure(Throwable failure) {
                System.out.println("failure:" + failure);
            }
        }, system.dispatcher());
    }
}
