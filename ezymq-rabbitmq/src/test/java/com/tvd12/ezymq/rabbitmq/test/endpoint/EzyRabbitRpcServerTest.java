package com.tvd12.ezymq.rabbitmq.test.endpoint;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownSignalException;
import com.tvd12.ezyfox.util.EzyThreads;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitBufferConsumer;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitRpcServer;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

public class EzyRabbitRpcServerTest extends BaseTest {

    @Test
    public void startWithShutdownSignalExceptionTest() throws Exception {
        // given
        String exchange = RandomUtil.randomShortAlphabetString();
        String replyRoutingKey = RandomUtil.randomShortAlphabetString();
        String requestQueueName = RandomUtil.randomShortAlphabetString();
        Channel channel = mock(Channel.class);

        EzyRabbitRpcServer server = EzyRabbitRpcServer.builder()
            .channel(channel)
            .exchange(exchange)
            .queueName(requestQueueName)
            .replyRoutingKey(replyRoutingKey)
            .build();

        Method method = mock(Method.class);
        ShutdownSignalException exception = new ShutdownSignalException(
            false,
            false,
            method,
            this,
            "test",
            null
        );
        EzyRabbitBufferConsumer consumer = FieldUtil.getFieldValue(
            server,
            "consumer"
        );
        consumer.handleShutdownSignal("test", exception);

        // when
        server.start();

        // then
        Asserts.assertFalse(
            FieldUtil.getFieldValue(server, "active")
        );
    }

    @Test
    public void startWithExceptionTest() throws Exception {
        // given
        String exchange = RandomUtil.randomShortAlphabetString();
        String replyRoutingKey = RandomUtil.randomShortAlphabetString();
        String requestQueueName = RandomUtil.randomShortAlphabetString();
        Channel channel = mock(Channel.class);

        EzyRabbitRpcServer server = EzyRabbitRpcServer.builder()
            .channel(channel)
            .exchange(exchange)
            .queueName(requestQueueName)
            .replyRoutingKey(replyRoutingKey)
            .build();

        RuntimeException exception = new RuntimeException("test");
        EzyRabbitBufferConsumer consumer = FieldUtil.getFieldValue(
            server,
            "consumer"
        );
        FieldUtil.setFieldValue(consumer, "exception", exception);
        consumer.close();
        Thread newThread = new Thread(() -> {
            while (true) {
                try {
                    EzyThreads.sleep(100);
                    FieldUtil.setFieldValue(consumer, "exception", null);
                    consumer.close();
                } catch (Throwable e) {
                    e.printStackTrace();
                    break;
                }
            }
        });
        newThread.start();

        // when
        server.start();

        // then
        Asserts.assertThatThrows(server::start)
                .isEqualsType(IllegalStateException.class);
        Asserts.assertFalse(
            FieldUtil.getFieldValue(server, "active")
        );
        server.close();
        newThread.interrupt();
    }
}
