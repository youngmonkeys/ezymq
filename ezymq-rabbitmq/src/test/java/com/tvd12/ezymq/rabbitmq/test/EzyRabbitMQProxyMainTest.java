package com.tvd12.ezymq.rabbitmq.test;

import com.tvd12.ezymq.rabbitmq.EzyRabbitMQProxy;
import com.tvd12.ezymq.rabbitmq.EzyRabbitRpcProducer;
import com.tvd12.ezymq.rabbitmq.test.request.SumRequest;
import com.tvd12.ezymq.rabbitmq.test.response.SumResponse;

public class EzyRabbitMQProxyMainTest {

    public static void main(String[] args) {
        EzyRabbitMQProxy proxy = EzyRabbitMQProxy.builder()
            .scan("com.tvd12.ezymq.rabbitmq.test")
            .settingsBuilder()
            .topicSettingBuilder("test")
            .exchange("sum-topic-exchange")
            .producerEnable(true)
            .clientRoutingKey("sum-topic-routing-key")
            .consumerEnable(true)
            .serverQueueName("sum-topic")
            .parent()
            .rpcProducerSettingBuilder("test")
            .exchange("sum-rpc-exchange")
            .requestQueueName("sum-rpc-queue")
            .requestRoutingKey("sum-rpc-routing-key")
            .replyQueueName("sum-rpc-client-queue")
            .replyRoutingKey("sum-rpc-client-routing-key")
            .parent()
            .rpcConsumerSettingBuilder("fibonacci")
            .requestQueueName("sum-rpc-queue")
            .exchange("sum-rpc-exchange")
            .replyRoutingKey("sum-rpc-client-routing-key")
            .parent()
            .parent()
            .build();
        EzyRabbitRpcProducer producer = proxy.getRpcProducer("test");
        SumResponse sumResponse = producer.call(
            "sum",
            new SumRequest(1, 2),
            SumResponse.class
        );
        System.out.println(sumResponse);
    }
}
