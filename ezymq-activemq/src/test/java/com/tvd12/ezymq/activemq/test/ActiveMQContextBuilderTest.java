package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezymq.activemq.EzyActiveMQProxy;
import com.tvd12.ezymq.activemq.EzyActiveRpcProducer;
import com.tvd12.ezymq.activemq.EzyActiveTopic;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMQContextBuilderTest extends ActiveMQBaseTest {

    protected static Logger logger =
        LoggerFactory.getLogger(ActiveMQContextBuilderTest.class);

    public static void main(String[] args) throws Exception {
        EzyActiveMQProxy context = EzyActiveMQProxy.builder()
            .scan("com.tvd12.ezymq.activemq.test.entity")
            .mapRequestType("fibonacci", int.class)
            .mapRequestType("test", String.class)
            .mapRequestType("", String.class)
            .settingsBuilder()
            .topicSettingBuilder("test")
            .clientEnable(true)
            .topicName("topic-test")
            .serverEnable(true)
            .serverThreadPoolSize(3)
            .parent()
            .rpcProducerSettingBuilder("fibonacci")
            .defaultTimeout(300 * 1000)
            .requestQueueName("rpc-request-test-1")
            .replyQueueName("rpc-response-test-1")
            .parent()
            .rpcConsumerSettingBuilder("fibonacci")
            .requestQueueName("rpc-request-test-1")
            .replyQueueName("rpc-response-test-1")
            .addRequestHandler("fibonacci", new EzyActiveRequestHandler<Integer>() {
                @Override
                public Object handle(Integer request) throws Exception {
                    return request + 1;
                }
            })
            .parent()
            .parent()
            .build();
        EzyActiveTopic<String> topic = context.getTopic("test");
        topic.addConsumer(message -> logger.info("topic message: " + message));
        long startTopicTime = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            topic.publish("hello topic " + i);
        }
        long elapsedTopicTime = System.currentTimeMillis() - startTopicTime;
        System.out.println("elapsedTopicTime: " + elapsedTopicTime);
        EzyActiveRpcProducer consumer = context.getRpcProducer("fibonacci");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
//			System.out.println("rabbit rpc start call: " + i);
            consumer.call("fibonacci", 100, int.class);
//			int result = consumer.call("fibonacci", 100, int.class);
//			System.out.println("i = " + i + ", result = " + result);
        }
        System.out.println("elapsed = " + (System.currentTimeMillis() - start));
        context.close();
        //noinspection InfiniteLoopStatement
        while (true) {
            //noinspection BusyWait
            Thread.sleep(100);
        }
    }
}
