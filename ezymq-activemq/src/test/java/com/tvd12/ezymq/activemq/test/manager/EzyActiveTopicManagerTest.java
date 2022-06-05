package com.tvd12.ezymq.activemq.test.manager;

import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.activemq.manager.EzyActiveTopicManager;
import com.tvd12.ezymq.activemq.setting.EzyActiveTopicSetting;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.jms.*;
import java.util.Map;

import static org.mockito.Mockito.*;

public class EzyActiveTopicManagerTest extends BaseTest {

    @SuppressWarnings("unchecked")
    @Test
    public void test() throws JMSException {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        String topicName = RandomUtil.randomShortAlphabetString();
        EzyActiveTopicSetting topicSetting = EzyActiveTopicSetting.builder()
            .topicName(topicName)
            .producerEnable(true)
            .consumerEnable(true)
            .build();
        Map<String, EzyActiveTopicSetting> topicSettings = EzyMapBuilder
            .mapBuilder()
            .put(topicName, topicSetting)
            .build();

        Connection connection = mock(Connection.class);
        when(connectionFactory.createConnection()).thenReturn(connection);

        Session session = mock(Session.class);
        when(
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
        ).thenReturn(session);

        MessageProducer producer = mock(MessageProducer.class);
        when(session.createProducer(any())).thenReturn(producer);

        MessageConsumer consumer = mock(MessageConsumer.class);
        when(session.createConsumer(any())).thenReturn(consumer);

        // when
        EzyActiveTopicManager sut = new EzyActiveTopicManager(
            dataCodec,
            connectionFactory,
            topicSettings
        );

        // then
        Asserts.assertNotNull(
            sut.getTopic(topicName)
        );
        Asserts.assertThatThrows(() -> sut.getTopic("not found"))
            .isEqualsType(IllegalArgumentException.class);

        verify(connectionFactory, times(1)).createConnection();
        verify(connection, times(1)).start();
        verify(
            connection, times(1)
        ).createSession(false, Session.AUTO_ACKNOWLEDGE);
        verify(session, times(1)).createProducer(any());
        verify(session, times(1)).createConsumer(any());

        sut.close();
        verify(producer, times(1)).close();
        verify(consumer, times(1)).close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void createTopicFailed() throws JMSException {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        String topicName = RandomUtil.randomShortAlphabetString();
        EzyActiveTopicSetting topicSetting = EzyActiveTopicSetting.builder()
            .topicName(topicName)
            .producerEnable(true)
            .consumerEnable(true)
            .build();
        Map<String, EzyActiveTopicSetting> topicSettings = EzyMapBuilder
            .mapBuilder()
            .put(topicName, topicSetting)
            .build();

        Connection connection = mock(Connection.class);
        when(connectionFactory.createConnection()).thenReturn(connection);

        Session session = mock(Session.class);
        when(
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
        ).thenReturn(session);

        MessageProducer producer = mock(MessageProducer.class);
        when(session.createProducer(any())).thenReturn(producer);

        RuntimeException exception = new RuntimeException("test");
        when(session.createConsumer(any())).thenThrow(exception);

        // when
        Throwable e = Asserts.assertThrows(() ->
            new EzyActiveTopicManager(
                dataCodec,
                connectionFactory,
                topicSettings
            )
        );

        // then
        Asserts.assertEqualsType(e, java.lang.IllegalStateException.class);

        verify(connectionFactory, times(1)).createConnection();
        verify(connection, times(1)).start();
        verify(
            connection, times(1)
        ).createSession(false, Session.AUTO_ACKNOWLEDGE);
        verify(session, times(1)).createProducer(any());
        verify(session, times(1)).createConsumer(any());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void buildWithDisableProducerAndConsumer() throws JMSException {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        String topicName = RandomUtil.randomShortAlphabetString();
        EzyActiveTopicSetting topicSetting = EzyActiveTopicSetting.builder()
            .topicName(topicName)
            .build();
        Map<String, EzyActiveTopicSetting> topicSettings = EzyMapBuilder
            .mapBuilder()
            .put(topicName, topicSetting)
            .build();

        Connection connection = mock(Connection.class);
        when(connectionFactory.createConnection()).thenReturn(connection);

        Session session = mock(Session.class);
        when(
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
        ).thenReturn(session);

        MessageProducer producer = mock(MessageProducer.class);
        when(session.createProducer(any())).thenReturn(producer);

        MessageConsumer consumer = mock(MessageConsumer.class);
        when(session.createConsumer(any())).thenReturn(consumer);

        // when
        EzyActiveTopicManager sut = new EzyActiveTopicManager(
            dataCodec,
            connectionFactory,
            topicSettings
        );

        // then
        Asserts.assertNotNull(
            sut.getTopic(topicName)
        );
        Asserts.assertThatThrows(() -> sut.getTopic("not found"))
            .isEqualsType(IllegalArgumentException.class);

        verify(connectionFactory, times(1)).createConnection();
        verify(connection, times(1)).start();
        verify(
            connection, times(1)
        ).createSession(false, Session.AUTO_ACKNOWLEDGE);
    }
}
