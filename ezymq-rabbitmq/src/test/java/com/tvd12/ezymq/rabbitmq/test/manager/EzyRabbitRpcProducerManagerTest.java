package com.tvd12.ezymq.rabbitmq.test.manager;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitRpcProducerManager;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitRpcProducerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;

public class EzyRabbitRpcProducerManagerTest extends BaseTest {

    @Test
    public void createRpcProducerFailedTest() throws IOException, TimeoutException {
        // given
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Map<String, Map<String, Object>> queueArguments = new HashMap<>();
        String producerName = RandomUtil.randomShortAlphabetString();
        EzyRabbitRpcProducerSetting setting = EzyRabbitRpcProducerSetting.builder()
            .build();
        Map<String, EzyRabbitRpcProducerSetting> rpcProducerSettings =
            Collections.singletonMap(producerName, setting);

        RuntimeException exception = new RuntimeException("test");
        when(connectionFactory.newConnection()).thenThrow(exception);

        // when
        Throwable e = Asserts.assertThrows(() ->
            new EzyRabbitRpcProducerManager(
                entityCodec,
                connectionFactory,
                queueArguments,
                rpcProducerSettings
            )
        );

        // then
        Asserts.assertEquals(e.getCause(), exception);
        verify(connectionFactory, times(1)).newConnection();
    }

    @Test
    public void getRpcProducerFailedTest() throws IOException, TimeoutException {
        // given
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Map<String, Map<String, Object>> queueArguments = new HashMap<>();
        String producerName = RandomUtil.randomShortAlphabetString();
        EzyRabbitRpcProducerSetting setting = EzyRabbitRpcProducerSetting.builder()
            .build();
        Map<String, EzyRabbitRpcProducerSetting> rpcProducerSettings =
            Collections.singletonMap(producerName, setting);

        Connection connection = mock(Connection.class);
        when(connectionFactory.newConnection()).thenReturn(connection);

        Channel channel = mock(Channel.class);
        when(connection.createChannel()).thenReturn(channel);

        // when
        EzyRabbitRpcProducerManager sut = new EzyRabbitRpcProducerManager(
            entityCodec,
            connectionFactory,
            queueArguments,
            rpcProducerSettings
        );

        // then
        Asserts.assertThatThrows(() -> sut.getRpcProducer("not found"))
                .isEqualsType(IllegalArgumentException.class);
        verify(connectionFactory, times(1)).newConnection();
        verify(connection, times(1)).createChannel();
    }
}
