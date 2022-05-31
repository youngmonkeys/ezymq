package com.tvd12.ezymq.rabbitmq.test;

import com.tvd12.ezymq.rabbitmq.EzyRabbitMQProxy;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcProducer;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.test.request.SumRequest;
import com.tvd12.ezymq.rabbitmq.test.response.SumResponse;

public class EzyRabbitMQProxyMainTest {

    public static void main(String[] args) {
        EzyRabbitMQProxy proxy = EzyRabbitMQProxy.builder()
            .scan("com.tvd12.ezymq.rabbitmq.test")
            .mapTopicMessageType("hello", "hello", SumRequest.class)
            .build();
        EzyRabbitRpcProducer producer = proxy.getRpcProducer("test");
        SumResponse sumResponse = producer.call(
            "sum",
            new SumRequest(1, 2),
            SumResponse.class
        );
        System.out.println(sumResponse);

        EzyRabbitTopic<SumRequest> sumTopic = proxy.getTopic("hello");
        sumTopic.addConsumer("hello", message ->
            System.out.println("sum request: " + message)
        );
        sumTopic.publish("hello", new SumRequest(1, 2));
        sumTopic.publish("sumMessage", new SumRequest(1, 2));
    }
}
