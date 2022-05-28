package com.tvd12.ezymq.rabbitmq.testing;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcClient;
import com.tvd12.ezymq.rabbitmq.handler.EzyRabbitResponseConsumer;
import com.tvd12.ezymq.rabbitmq.testing.mockup.ChannelMockup;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import java.util.concurrent.TimeoutException;

public class EzyRabbitRpcClientTest extends BaseTest {

    @Test
    public void test() throws Exception {
        Channel channel = new ChannelMockup();
        channel.queueDeclare("EzyRabbitRpcClientTest-replyQueueName", false, false, false, null);
        EzyRabbitRpcClient client = newClient(channel, 10);
        try {
            client.doCall(null, new byte[]{1, 2, 3});
        } catch (Exception e) {
            assert e instanceof TimeoutException;
        }
        EzyRabbitRpcClient client2 = newClient(channel, 250);
        Thread[] threads = new Thread[5];
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(() -> {
                try {
                    client2.doCall(null, new byte[]{1, 2, 3});
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        for (int i = 0; i < threads.length; ++i) {
            threads[i].start();
        }
        Thread.sleep(500);

        EzyRabbitRpcClient client3 = newClient(channel, 3000);
        Thread.sleep(100);
        channel.basicPublish(
            "EzyRabbitRpcClientTest-exchange",
            "EzyRabbitRpcClientTest-routingKey", null, new byte[]{1, 2, 3});
        Thread.sleep(100);
        threads = new Thread[2];
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(() -> {
                try {
                    client3.doCall(null, new byte[]{1, 2, 3});
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        for (int i = 0; i < threads.length; ++i) {
            threads[i].start();
        }
        Thread.sleep(500);
        channel.basicCancel("shutdown_EzyRabbitRpcClientTest-replyQueueName");
        Thread.sleep(500);
    }

    protected EzyRabbitRpcClient newClient(Channel channel, int timeout) throws Exception {
        return new EzyRabbitRpcClient(
            channel,
            "EzyRabbitRpcClientTest-exchange",
            "EzyRabbitRpcClientTest-routingKey",
            "EzyRabbitRpcClientTest-replyQueueName",
            "EzyRabbitRpcClientTest-replyRoutingKey",
            3,
            timeout,
            new EzyRabbitResponseConsumer() {

                @Override
                public void consume(BasicProperties properties, byte[] responseBody) {
                    System.out.println("EzyRabbitResponseConsumer: " + properties + ", " + responseBody);
                }
            });
    }

}
