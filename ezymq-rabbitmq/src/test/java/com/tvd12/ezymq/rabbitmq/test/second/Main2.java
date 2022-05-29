package com.tvd12.ezymq.rabbitmq.test.second;

import com.rabbitmq.client.*;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.RpcClient.Response;

public class Main2 {

    public static void main(String[] args) throws Exception {
        Thread serverThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    System.out.println(Thread.currentThread().getName() + ": start server");
                    newRpcServer().mainloop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        serverThread.start();
        System.out.println(Thread.currentThread().getName() + ": rpc server");

        Thread.sleep(1000L);

        System.out.println(Thread.currentThread().getName() + ": rpc server after sleep");

        RpcClient client = newRpcClient();

        String replyQueueName = client.getChannel().queueDeclare().getQueue();

        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
            .replyTo(replyQueueName)
            .correlationId("1")
            .build();
        Response response = client.doCall(props, "hello-world".getBytes());
        System.out.println(new String(response.getBody()));
    }

    private static RpcClient newRpcClient() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare("my-exchange", "direct");
        channel.queueBind("rpc_queue", "my-exchange", "rpc_queue");
        RpcClientParams params = new RpcClientParams()
            .channel(channel)
            .exchange("")
            .routingKey("rpc_queue")
            .replyTo("rpc_queue")
            .timeout(3 * 1000);
        return new RpcClient(params);
    }

    private static RpcServer newRpcServer() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare("rpc_queue", false, false, false, null);
        channel.basicQos(1);
        return new RpcServer(channel, "rpc_queue") {
            public byte[] handleCall(byte[] requestBody, BasicProperties replyProperties) {
                return (new String(requestBody) + "#FromServer").getBytes();
            }

            ;
        };
    }

}
