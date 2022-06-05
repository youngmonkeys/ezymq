package com.tvd12.ezymq.activemq.test.manager;

import com.tvd12.ezyfox.util.EzyMapBuilder;
import com.tvd12.ezymq.activemq.manager.EzyActiveRpcConsumerManager;
import com.tvd12.ezymq.activemq.setting.EzyActiveRpcConsumerSetting;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.jms.*;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class EzyActiveRpcConsumerManagerTest extends BaseTest {

    @Test
    public void test() throws JMSException {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

        String requestQueueName = RandomUtil.randomShortAlphabetString();
        String replyQueueName = RandomUtil.randomShortAlphabetString();
        EzyActiveRpcConsumerSetting consumerSetting = EzyActiveRpcConsumerSetting
            .builder()
            .requestQueueName(requestQueueName)
            .replyQueueName(replyQueueName)
            .build();
        String consumerName = RandomUtil.randomShortAlphabetString();
        Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings =
            EzyMapBuilder.mapBuilder()
                .put(consumerName, consumerSetting)
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
        EzyActiveRpcConsumerManager sut = new EzyActiveRpcConsumerManager(
            dataCodec,
            connectionFactory,
            rpcConsumerSettings
        );

        // then
        Asserts.assertNotNull(
            sut.getRpcConsumer(consumerName)
        );
        Asserts.assertThatThrows(() -> sut.getRpcConsumer("not found"))
                .isEqualsType(IllegalArgumentException.class);
        verify(connectionFactory, times(1)).createConnection();
        verify(connection, times(1)).start();
        verify(
            connection, times(1)
        ).createSession(false, Session.AUTO_ACKNOWLEDGE);
        verify(session, times(1)).createConsumer(any());
        verify(session, times(1)).createConsumer(any());
        sut.close();
        verify(producer, times(1)).close();
        verify(consumer, times(1)).close();
    }

    @Test
    public void createRpcConsumerFailed() throws JMSException {
        // given
        EzyMQDataCodec dataCodec = mock(EzyMQDataCodec.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

        String requestQueueName = RandomUtil.randomShortAlphabetString();
        String replyQueueName = RandomUtil.randomShortAlphabetString();
        EzyActiveRpcConsumerSetting consumerSetting = EzyActiveRpcConsumerSetting
            .builder()
            .requestQueueName(requestQueueName)
            .replyQueueName(replyQueueName)
            .build();
        String consumerName = RandomUtil.randomShortAlphabetString();
        Map<String, EzyActiveRpcConsumerSetting> rpcConsumerSettings =
            EzyMapBuilder.mapBuilder()
                .put(consumerName, consumerSetting)
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
            new EzyActiveRpcConsumerManager(
                dataCodec,
                connectionFactory,
                rpcConsumerSettings
            )
        );

        // then
        Asserts.assertEqualsType(e, java.lang.IllegalStateException.class);
        verify(connectionFactory, times(1)).createConnection();
        verify(connection, times(1)).start();
        verify(
            connection, times(1)
        ).createSession(false, Session.AUTO_ACKNOWLEDGE);
        verify(session, times(1)).createConsumer(any());
        verify(session, times(1)).createConsumer(any());
    }
}
