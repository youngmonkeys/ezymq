package com.tvd12.ezymq.rabbitmq.testing;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcServer;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitRpcCallHandler;
import com.tvd12.ezymq.rabbitmq.testing.mockup.ChannelMockup;
import org.testng.annotations.Test;

public class EzyRabbitRpcServerTest {

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
            .callHandler(new EzyRabbitRpcCallHandler() {
                @Override
                public void handleFire(BasicProperties requestProperties, byte[] requestBody) {

                }

                @Override
                public byte[] handleCall(BasicProperties requestProperties, byte[] requestBody, Builder replyPropertiesBuilder) {
                    return new byte[0];
                }
            })
            .build();
        channel.basicPublish(
            "EzyRabbitRpcServerTest.exchange",
            "EzyRabbitRpcServerTest.requestRoutingKey",
            new BasicProperties.Builder().build(), new byte[]{1, 2, 3});
        channel.basicCancel("cancel_EzyRabbitRpcServerTest.queueName");
        server.start();
        channel.basicCancel("shutdown_EzyRabbitRpcServerTest.queueName");
        server.start();
        System.out.println("set up server done");
        Thread.sleep(100);
    }

}
