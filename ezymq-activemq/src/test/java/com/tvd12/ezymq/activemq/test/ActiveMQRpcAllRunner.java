package com.tvd12.ezymq.activemq.test;

import com.tvd12.ezyfox.builder.EzyArrayBuilder;
import com.tvd12.ezyfox.factory.EzyEntityFactory;
import com.tvd12.ezymq.activemq.EzyActiveRpcConsumer;
import com.tvd12.ezymq.activemq.EzyActiveRpcProducer;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcClient;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcServer;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandler;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestHandlers;
import com.tvd12.ezymq.activemq.handler.EzyActiveRequestInterceptors;

import javax.jms.Connection;
import javax.jms.Session;

public class ActiveMQRpcAllRunner extends ActiveMQBaseTest {

    private final EzyActiveRequestHandlers requestHandlers;

    public ActiveMQRpcAllRunner() {
        this.requestHandlers = new EzyActiveRequestHandlers();
        this.requestHandlers.addHandler(
            "fibonacci",
            new EzyActiveRequestHandler<Integer>() {
                @Override
                public Object handle(Integer request) {
                    return request + 3;
                }
            });
    }

    public static void main(String[] args) throws Exception {
        EzyEntityFactory.create(EzyArrayBuilder.class);
        ActiveMQRpcAllRunner runner = new ActiveMQRpcAllRunner();
        runner.startServer();
        runner.sleep();
        runner.rpc();
    }

    @SuppressWarnings("resource")
    protected void startServer() {
        try {
            System.out.println("thread-" + Thread.currentThread().getName() + ": start server");
            EzyActiveRpcServer server = newServer();
            new EzyActiveRpcConsumer(
                dataCodec,
                server,
                requestHandlers,
                new EzyActiveRequestInterceptors()
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void sleep() throws Exception {
        Thread.sleep(1000);
    }

    @SuppressWarnings("resource")
    protected void rpc() throws Exception {
        EzyActiveRpcClient client = newClient();
        EzyActiveRpcProducer consumer = new EzyActiveRpcProducer(client, entityCodec);
        System.out.println("thread-" + Thread.currentThread().getName() + ": start rpc");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            System.out.println("rabbit rpc start call: " + i);
            int result = consumer.call("fibonacci", 100, int.class);
            System.out.println("i = " + i + ", result = " + result);
        }
        System.out.println("elapsed = " + (System.currentTimeMillis() - start));
    }

    protected EzyActiveRpcClient newClient() throws Exception {
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return EzyActiveRpcClient.builder()
            .session(session)
            .defaultTimeout(300 * 1000)
            .requestQueueName("rpc-request-test-1")
            .replyQueueName("rpc-response-test-1")
            .build();
    }

    protected EzyActiveRpcServer newServer() throws Exception {
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        return EzyActiveRpcServer.builder()
            .session(session)
            .requestQueueName("rpc-request-test-1")
            .replyQueueName("rpc-response-test-1")
            .build();
    }
}
