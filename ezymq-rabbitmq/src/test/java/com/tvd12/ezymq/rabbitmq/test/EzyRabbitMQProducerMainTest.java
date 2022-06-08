package com.tvd12.ezymq.rabbitmq.test;

import com.tvd12.ezyfox.util.EzyThreads;
import com.tvd12.ezymq.rabbitmq.EzyRabbitMQProxy;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.test.request.SumRequest;

public class EzyRabbitMQProducerMainTest {

    public static void main(String[] args) {
        EzyRabbitMQProxy proxy = EzyRabbitMQProxy.builder()
            .scan("com.tvd12.ezymq.rabbitmq.test")
            .mapTopicMessageType("hello", "hello", SumRequest.class)
            .build();

        EzyRabbitTopic<SumRequest> sumTopic = proxy.getTopic("hello");

        int a = 0;
        int b = 0;
        while (a < Integer.MAX_VALUE / 2) {
            sumTopic.publish("hello", new SumRequest(a, b));
            sumTopic.publish("sumMessage", new SumRequest(a, b));
            a ++;
            b ++;
            EzyThreads.sleep(1000);
        }
    }
}
