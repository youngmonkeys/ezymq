package com.tvd12.ezymq.activemq.test.manager;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.activemq.manager.EzyActiveRpcProducerManager;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcProducerSetting;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.jms.*;

import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

public class EzyActiveRpcProducerManagerTest extends BaseTest {

    @Test
    public void test() throws JMSException {
        // given
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

        String requestQueueName = RandomUtil.randomShortAlphabetString();
        String replyQueueName = RandomUtil.randomShortAlphabetString();
        EzyActiveRpcProducerSetting producerSetting = EzyActiveRpcProducerSetting
            .builder()
            .requestQueueName(requestQueueName)
            .replyQueueName(replyQueueName)
            .build();
        String producerName = RandomUtil.randomShortAlphabetString();
        Map<String, EzyActiveRpcProducerSetting> rpcProducerSettings =
            EzyMapBuilder.mapBuilder()
                .put(producerName, producerSetting)
                .toMap();

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
        EzyActiveRpcProducerManager sut = new EzyActiveRpcProducerManager(
            entityCodec,
            connectionFactory,
            rpcProducerSettings
        );

        // then
        Asserts.assertNotNull(
            sut.getRpcProducer(producerName)
        );
        Asserts.assertThatThrows(() -> sut.getRpcProducer("not found"))
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

    @Test
    public void createRpcProducerFailed() throws JMSException {
        // given
        EzyEntityCodec entityCodec = mock(EzyEntityCodec.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

        String requestQueueName = RandomUtil.randomShortAlphabetString();
        String replyQueueName = RandomUtil.randomShortAlphabetString();
        EzyActiveRpcProducerSetting producerSetting = EzyActiveRpcProducerSetting
            .builder()
            .requestQueueName(requestQueueName)
            .replyQueueName(replyQueueName)
            .build();
        String producerName = RandomUtil.randomShortAlphabetString();
        Map<String, EzyActiveRpcProducerSetting> rpcProducerSettings =
            EzyMapBuilder.mapBuilder()
                .put(producerName, producerSetting)
                .toMap();

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
            new EzyActiveRpcProducerManager(
                entityCodec,
                connectionFactory,
                rpcProducerSettings
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
}
