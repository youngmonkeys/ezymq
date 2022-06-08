package com.tvd12.ezymq.rabbitmq.test;

import com.tvd12.ezymq.rabbitmq.EzyRabbitMQProxy;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.test.request.SumRequest;

public class EzyRabbitMQConsumer2MainTest {

    public static void main(String[] args) {
        System.setProperty("active_profiles", "consumer-2");
        EzyRabbitMQProxy proxy = EzyRabbitMQProxy.builder()
            .scan("com.tvd12.ezymq.rabbitmq.test")
            .mapTopicMessageType("hello", "hello", SumRequest.class)
            .build();

        EzyRabbitTopic<SumRequest> sumTopic = proxy.getTopic("hello");
        sumTopic.addConsumer("hello", message ->
            System.out.println("sum request: " + message)
        );
    }
}
