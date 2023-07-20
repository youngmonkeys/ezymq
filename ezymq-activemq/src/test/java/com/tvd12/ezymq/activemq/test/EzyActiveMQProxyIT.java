package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezymq.activemq.EzyActiveMQProxy;
import com.tvd12.ezymq.activemq.EzyActiveRpcProducer;
import com.tvd12.ezymq.activemq.EzyActiveTopic;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import com.tvd12.ezymq.activemq.setting.EzyActiveSettings;

public class EzyActiveMQProxyIT {

    private final EzyActiveMQProxy activeMQProxy;

    public EzyActiveMQProxyIT() {
        EzyActiveSettings settings = EzyActiveSettings.builder()
            .mapRequestType("fibonacci", Integer.class)
            .mapTopicMessageType("test", "hello", String.class)
            .rpcProducerSettingBuilder("test")
            .defaultTimeout(300 * 1000)
            .requestQueueName("rpc-request-test-1")
            .replyQueueName("rpc-response-test-1")
            .parent()
            .rpcConsumerSettingBuilder("test")
            .requestQueueName("rpc-request-test-1")
            .replyQueueName("rpc-response-test-1")
            .addRequestHandler(
                "fibonacci",
                new EzyActiveRequestHandler<Integer>() {
                    @Override
                    public Object handle(Integer request) {
                        return request + 3;
                    }
                })
            .parent()
            .topicSettingBuilder("test")
            .consumerEnable(true)
            .producerEnable(true)
            .topicName("test")
            .parent()
            .build();
        this.activeMQProxy = EzyActiveMQProxy.builder()
            .settings(settings)
            .build();
    }

    public void test() {
        EzyActiveRpcProducer producer = activeMQProxy.getRpcProducer("test");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; ++i) {
            System.out.println("rabbit rpc start call: " + i);
            int result = producer.call("fibonacci", 100, int.class);
            System.out.println("i = " + i + ", result = " + result);
        }
        System.out.println("elapsed = " + (System.currentTimeMillis() - start));

        EzyActiveTopic<String> topic = activeMQProxy.getTopic("test");
        topic.addConsumer("hello", it ->
            System.out.println("topic received: " + it)
        );
        topic.publish("hello", "Young Monkeys");
    }

    public static void main(String[] args) throws Exception {
        new EzyActiveMQProxyIT().test();
    }
}

