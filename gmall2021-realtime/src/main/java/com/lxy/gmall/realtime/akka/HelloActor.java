package com.lxy.gmall.realtime.akka;

/**
 * @author lxy
 * @date 2021/7/19
 * <p>
 * 获取Actor:
 * 若提供了Actor的路径，可以通过路径获取到ActorRef，然后与之通信
 */
public class HelloActor {

    private void handleMessage(Object object) {
        if (object instanceof String) {
            String str = (String) object;
//            log.info("[HelloActor] message is {}, sender is {}", str, getSender().path().toString());
// 给发送者发送消息
//            getSender().tell(str, getSelf());
        }
    }
}
