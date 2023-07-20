package com.tvd12.ezymq.rabbitmq.test;

import com.tvd12.ezymq.rabbitmq.EzyRabbitMQProxy;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcProducer;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRequestHandler;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitSettings;

public class EzyRabbitMQProxyIT {

    private final EzyRabbitMQProxy rabbitMQProxy;

    public EzyRabbitMQProxyIT() {
        EzyRabbitSettings settings = EzyRabbitSettings.builder()
            .mapRequestType("fibonacci", Integer.class)
            .mapTopicMessageType("test", "hello", String.class)
            .rpcProducerSettingBuilder("test")
            .defaultTimeout(300 * 1000)
            .exchange("rmqia-rpc-exchange")
            .replyRoutingKey("rmqia-rpc-routing-key")
            .replyQueueName("rmqia-rpc-client-queue-private")
            .replyRoutingKey("rmqia-rpc-client-routing-key-private")
            .parent()
            .rpcConsumerSettingBuilder("test")
            .requestQueueName("rmqia-rpc-queue")
            .exchange("rmqia-rpc-exchange")
            .replyRoutingKey("rmqia-rpc-client-routing-key")
            .addRequestHandler(
                "fibonacci",
                new EzyRabbitRequestHandler<Integer>() {
                    @Override
                    public Object handle(Integer request) {
                        return request + 3;
                    }
                }
            )
            .parent()
            .topicSettingBuilder("test")
            .exchange("rmqia-topic-exchange")
            .producerEnable(true)
            .producerRoutingKey("rmqia-topic-routing-key")
            .consumerEnable(true)
            .consumerQueueName("mqia-topic")
            .parent()
            .build();
        rabbitMQProxy = EzyRabbitMQProxy.builder()
            .settings(settings)
            .build();
    }

    public void test() {
        EzyRabbitRpcProducer producer = rabbitMQProxy.getRpcProducer("test");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            System.out.println("rabbit rpc start call: " + i);
            int result = producer.call("fibonacci", 100, int.class);
            System.out.println("i = " + i + ", result = " + result);
        }
        System.out.println("elapsed = " + (System.currentTimeMillis() - start));

        EzyRabbitTopic<String> topic = rabbitMQProxy.getTopic("test");
        topic.addConsumer("hello", (message) ->
            System.out.println("topic response: " + message)
        );
        topic.publish("hello", "Young Monkeys");
    }

    public static void main(String[] args) throws Exception {
        new EzyRabbitMQProxyIT().test();
    }
}
