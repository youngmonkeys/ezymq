package com.tvd12.ezymq.rabbitmq.test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.tvd12.ezymq.rabbitmq.EzyRabbitTopic;
import com.tvd12.ezymq.rabbitmq.constant.EzyRabbitExchangeTypes;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicClient;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitTopicServer;

public class EzyRabbitTopicMainTest extends RabbitBaseTest {

    public static void main(String[] args) throws Exception {
        new EzyRabbitTopicMainTest().test();
    }

    @SuppressWarnings("unchecked")
    public void test() throws Exception {
        String exchange = "test-topic-exchange";
        String routingKey1 = "test-topic-routing-key-1";
        String queue1 = "test-topic-queue-1";
        String queue2 = "test-topic-queue-2";
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.basicQos(1);
        channel.exchangeDeclare(exchange, EzyRabbitExchangeTypes.FANOUT);
        channel.queueDeclare(queue1, false, false, false, null);
        channel.queueDeclare(queue2, false, false, false, null);
        channel.queueBind(queue1, exchange, routingKey1);
        channel.queueBind(queue2, exchange, routingKey1);
        EzyRabbitTopicServer server = EzyRabbitTopicServer.builder()
            .channel(channel)
            .exchange(exchange)
            .queueName(queue1)
            .build();
        EzyRabbitTopicClient client = EzyRabbitTopicClient.builder()
            .channel(channel)
            .exchange(exchange)
            .routingKey(routingKey1)
            .build();
        EzyRabbitTopic<String> topic = EzyRabbitTopic.builder()
            .client(client)
            .server(server)
            .dataCodec(dataCodec)
            .build();
        topic.addConsumer("test", msg -> {
            System.out.println("received: " + msg);
        });
        System.out.println("start consumer ok");
        Thread.sleep(100);
        topic.publish("test", "I'm a monkey");
        System.out.println("publish ok");
    }

}
