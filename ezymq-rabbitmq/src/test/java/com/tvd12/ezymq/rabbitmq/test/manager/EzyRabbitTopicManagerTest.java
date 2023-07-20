package com.tvd12.ezymq.rabbitmq.test.manager;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitTopicManager;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitTopicSetting;
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

public class EzyRabbitTopicManagerTest extends BaseTest {

    @Test
    public void noProducerAndConsumerTest() throws IOException, TimeoutException {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        Map<String, Map<String, Object>> queueArguments = new HashMap<>();
        String topicName = RandomUtil.randomShortAlphabetString();
        EzyRabbitTopicSetting setting = EzyRabbitTopicSetting.builder()
            .build();
        Map<String, EzyRabbitTopicSetting> topicSettings =
            Collections.singletonMap(topicName, setting);

        Connection connection = mock(Connection.class);

        Channel channel = mock(Channel.class);
        when(connection.createChannel()).thenReturn(channel);

        // when
        EzyRabbitTopicManager sut = new EzyRabbitTopicManager(
            connection,
            dataCodec,
            queueArguments,
            topicSettings
        );

        // then
        Asserts.assertThatThrows(() -> sut.getTopic("not found"))
            .isEqualsType(IllegalArgumentException.class);
        verify(connection, times(1)).createChannel();
    }

    @Test
    public void createTopicFailedTest() throws IOException, TimeoutException {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        Map<String, Map<String, Object>> queueArguments = new HashMap<>();
        String topicName = RandomUtil.randomShortAlphabetString();
        EzyRabbitTopicSetting setting = EzyRabbitTopicSetting.builder()
            .build();
        Map<String, EzyRabbitTopicSetting> topicSettings =
            Collections.singletonMap(topicName, setting);

        RuntimeException exception = new RuntimeException("test");
        Connection connection = mock(Connection.class);
        when(connection.createChannel()).thenThrow(exception);

        // when
        Throwable e = Asserts.assertThrows(() ->
            new EzyRabbitTopicManager(
                connection,
                dataCodec,
                queueArguments,
                topicSettings
            )
        );

        // then
        Asserts.assertEquals(e.getCause(), exception);
        verify(connection, times(1)).createChannel();
    }
}
