package com.tvd12.ezymq.rabbitmq.test.endpoint;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.AddressResolver;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MetricsCollector;
import com.rabbitmq.client.impl.AMQConnection;
import com.rabbitmq.client.impl.ConnectionParams;
import com.rabbitmq.client.impl.FrameHandler;
import com.rabbitmq.client.impl.FrameHandlerFactory;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitConnectionFactory;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

public class EzyRabbitConnectionFactoryTest extends BaseTest {

    @Test
    public void newConnectionSuccessfullyButRetry() throws IOException, TimeoutException {
        // given
        int maxConnectionAttempts = RandomUtil.randomInt(2, 10);

        ExecutorService executorService = mock(ExecutorService.class);
        AddressResolver addressResolver = mock(AddressResolver.class);

        Address address = new Address("localhost", 12345);
        List<Address> resolved = Collections.singletonList(address);
        when(addressResolver.getAddresses()).thenReturn(resolved);

        String clientProvidedName = RandomUtil.randomShortAlphabetString();

        FrameHandlerFactory frameHandlerFactory = mock(FrameHandlerFactory.class);

        AMQConnection connection = mock(AMQConnection.class);

        AtomicInteger retryCount = new AtomicInteger();
        EzyRabbitConnectionFactory sut = new EzyRabbitConnectionFactory() {
            @Override
            protected AMQConnection createConnection(
                ConnectionParams params,
                FrameHandler frameHandler,
                MetricsCollector metricsCollector
            ) {
                if (retryCount.incrementAndGet() == 1) {
                    throw new IllegalStateException("test");
                }
                return connection;
            }

            protected synchronized FrameHandlerFactory createFrameHandlerFactory() {
                return frameHandlerFactory;
            }
        };
        sut.setAutomaticRecoveryEnabled(false);
        sut.setMaxConnectionAttempts(maxConnectionAttempts);

        // when
        Connection actual = sut.newConnection(
            executorService,
            addressResolver,
            clientProvidedName
        );

        // then
        Asserts.assertEquals(actual, connection);
    }

    @Test
    public void newConnectionFailed() throws IOException {
        // given
        int maxConnectionAttempts = 0;

        ExecutorService executorService = mock(ExecutorService.class);
        AddressResolver addressResolver = mock(AddressResolver.class);

        Address address = new Address("localhost", 12345);
        List<Address> resolved = Collections.singletonList(address);
        when(addressResolver.getAddresses()).thenReturn(resolved);

        String clientProvidedName = RandomUtil.randomShortAlphabetString();

        FrameHandlerFactory frameHandlerFactory = mock(FrameHandlerFactory.class);

        IllegalStateException exception = new IllegalStateException("test");
        EzyRabbitConnectionFactory sut = new EzyRabbitConnectionFactory() {
            @Override
            protected AMQConnection createConnection(
                ConnectionParams params,
                FrameHandler frameHandler,
                MetricsCollector metricsCollector
            ) {
                throw exception;
            }

            protected synchronized FrameHandlerFactory createFrameHandlerFactory() {
                return frameHandlerFactory;
            }
        };
        sut.setAutomaticRecoveryEnabled(false);
        sut.setMaxConnectionAttempts(maxConnectionAttempts);

        // when
        Throwable e = Asserts.assertThrows(() ->
            sut.newConnection(
                executorService,
                addressResolver,
                clientProvidedName
            )
        );

        // then
        Asserts.assertEquals(e, exception);
    }

    @Test
    public void closeTest() throws Exception {
        // given
        EzyRabbitConnectionFactory instance = new EzyRabbitConnectionFactory();
        List<Connection> createdConnections = FieldUtil.getFieldValue(
            instance,
            "createdConnections"
        );
        Connection connection = mock(Connection.class);
        createdConnections.add(connection);

        // when
        instance.close();

        // then
        verify(connection, times(1)).close();
        verifyNoMoreInteractions(connection);
    }
}
