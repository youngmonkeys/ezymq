package com.tvd12.ezymq.rabbitmq.test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcServer;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRpcCallHandler;
import com.tvd12.ezymq.rabbitmq.test.mockup.ChannelMockup;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

public class EzyRabbitRpcServerTest extends BaseTest {

    @Test
    public void test() throws Exception {
        ChannelMockup channel = new ChannelMockup();
        channel.queueBind(
            "EzyRabbitRpcServerTest.queueName",
            "EzyRabbitRpcServerTest.exchange",
            "EzyRabbitRpcServerTest.requestRoutingKey");
        EzyRabbitRpcServer server = EzyRabbitRpcServer.builder()
            .channel(channel)
            .exchange("EzyRabbitRpcServerTest.exchange")
            .replyRoutingKey("EzyRabbitRpcServerTest.replyRoutingKey")
            .queueName("EzyRabbitRpcServerTest.queueName")
            .build();
        channel.basicPublish(
            "EzyRabbitRpcServerTest.exchange",
            "EzyRabbitRpcServerTest.requestRoutingKey",
            new BasicProperties.Builder().build(), new byte[]{1, 2, 3});
        channel.basicCancel("cancel_EzyRabbitRpcServerTest.queueName");
        server.start();
        channel.basicCancel("shutdown_EzyRabbitRpcServerTest.queueName");
        System.out.println("set up server done");
        Thread.sleep(100);
        server.close();
    }
}
