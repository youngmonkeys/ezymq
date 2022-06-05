package com.tvd12.ezymq.activemq.test.endpoint;

import com.tvd12.ezymq.activemq.endpoint.EzyActiveTopicServer;
import com.tvd12.ezymq.activemq.handler.EzyActiveMessageHandler;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;
import com.tvd12.test.base.BaseTest;
import org.testng.annotations.Test;

import javax.jms.*;
import java.util.Enumeration;

import static org.mockito.Mockito.*;

public class EzyActiveTopicServerTest extends BaseTest {

    @SuppressWarnings("unchecked")
    @Test
    public void startTest() throws Exception {
        // given
        Session session = mock(Session.class);
        Destination topic = mock(Destination.class);
        int threadPoolSize = 1;

        MessageConsumer consumer = mock(MessageConsumer.class);
        when(session.createConsumer(topic)).thenReturn(consumer);

        EzyActiveTopicServer sut = EzyActiveTopicServer.builder()
            .session(session)
            .topic(topic)
            .threadPoolSize(threadPoolSize)
            .build();

        BytesMessage bytesMessage = mock(BytesMessage.class);
        when(consumer.receive()).thenReturn(bytesMessage);

        Enumeration<String> propertyNames = mock(Enumeration.class);
        when(bytesMessage.getPropertyNames()).thenReturn(propertyNames);

        EzyActiveMessageHandler handler = mock(EzyActiveMessageHandler.class);
        sut.setMessageHandler(handler);

        // when
        sut.start();
        Thread.sleep(100);
        sut.close();

        // then
        verify(session, times(1)).createConsumer(topic);
        verify(consumer, atLeast(1)).receive();
        verify(bytesMessage, atLeast(1)).getPropertyNames();
        verify(handler, atLeast(1)).handle(
            any(EzyActiveProperties.class),
            any(byte[].class)
        );
    }

    @Test
    public void loopButMessageIsNullTest() throws Exception {
        // given
        Session session = mock(Session.class);
        Destination topic = mock(Destination.class);
        int threadPoolSize = 1;

        MessageConsumer consumer = mock(MessageConsumer.class);
        when(session.createConsumer(topic)).thenReturn(consumer);

        EzyActiveTopicServer sut = EzyActiveTopicServer.builder()
            .session(session)
            .topic(topic)
            .threadPoolSize(threadPoolSize)
            .build();

        EzyActiveMessageHandler handler = mock(EzyActiveMessageHandler.class);
        sut.setMessageHandler(handler);

        // when
        sut.start();
        Thread.sleep(100);
        sut.close();

        // then
        verify(session, times(1)).createConsumer(topic);
        verify(consumer, atLeast(1)).receive();
    }

    @Test
    public void loopWithJMSException() throws Exception {
        // given
        Session session = mock(Session.class);
        Destination topic = mock(Destination.class);
        int threadPoolSize = 1;

        MessageConsumer consumer = mock(MessageConsumer.class);
        when(session.createConsumer(topic)).thenReturn(consumer);

        JMSException exception = new JMSException("test");
        when(consumer.receive()).thenThrow(exception);

        EzyActiveTopicServer sut = EzyActiveTopicServer.builder()
            .session(session)
            .topic(topic)
            .threadPoolSize(threadPoolSize)
            .build();

        EzyActiveMessageHandler handler = mock(EzyActiveMessageHandler.class);
        sut.setMessageHandler(handler);

        // when
        sut.start();
        Thread.sleep(100);
        sut.close();

        // then
        verify(session, times(1)).createConsumer(topic);
        verify(consumer, atLeast(1)).receive();
    }

    @Test
    public void loopWithThrowable() throws Exception {
        // given
        Session session = mock(Session.class);
        Destination topic = mock(Destination.class);
        int threadPoolSize = 1;

        MessageConsumer consumer = mock(MessageConsumer.class);
        when(session.createConsumer(topic)).thenReturn(consumer);

        RuntimeException exception = new RuntimeException("test");
        when(consumer.receive()).thenThrow(exception);

        EzyActiveTopicServer sut = EzyActiveTopicServer.builder()
            .session(session)
            .topic(topic)
            .threadPoolSize(threadPoolSize)
            .build();

        EzyActiveMessageHandler handler = mock(EzyActiveMessageHandler.class);
        sut.setMessageHandler(handler);

        // when
        sut.start();
        Thread.sleep(100);
        sut.close();

        // then
        verify(session, times(1)).createConsumer(topic);
        verify(consumer, atLeast(1)).receive();
    }
}
