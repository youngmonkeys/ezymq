package com.tvd12.ezymq.activemq.test.endpoint;

import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcServer;
import com.tvd12.ezymq.activemq.handler.EzyActiveRpcCallHandler;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.jms.*;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

public class EzyActiveRpcServerTest extends BaseTest {

    @SuppressWarnings("unchecked")
    @Test
    public void startTest() throws Exception {
        // given
        Destination requestQueue = mock(Destination.class);
        Destination replyQueue = mock(Destination.class);
        int threadPoolSize = 1;

        Session session = mock(Session.class);
        MessageProducer messageProducer = mock(MessageProducer.class);
        when(session.createProducer(replyQueue)).thenReturn(messageProducer);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        when(session.createConsumer(requestQueue)).thenReturn(messageConsumer);

        EzyActiveRpcCallHandler handler = mock(EzyActiveRpcCallHandler.class);

        EzyActiveRpcServer sut = EzyActiveRpcServer.builder()
            .session(session)
            .threadPoolSize(threadPoolSize)
            .requestQueue(requestQueue)
            .replyQueue(replyQueue)
            .build();
        sut.setCallHandler(handler);

        BytesMessage message = mock(BytesMessage.class);
        when(messageConsumer.receive()).thenReturn(message);

        BytesMessage bytesMessage = mock(BytesMessage.class);
        when(session.createBytesMessage()).thenReturn(bytesMessage);

        String messageType = RandomUtil.randomShortAlphabetString();
        String requestId = RandomUtil.randomShortAlphabetString();
        when(message.getJMSType()).thenReturn(messageType);
        when(message.getJMSCorrelationID()).thenReturn(requestId);

        Enumeration<String> propertyNames = mock(Enumeration.class);
        when(message.getPropertyNames()).thenReturn(propertyNames);

        AtomicReference<byte[]> requestBody = new AtomicReference<>();
        when(message.readBytes(any(byte[].class))).thenAnswer(it -> {
            if (it.getArguments().length > 0) {
                requestBody.set(it.getArgumentAt(0, byte[].class));
            }
            return 0;
        });

        byte[] responseBody = RandomUtil.randomShortByteArray();
        when(handler.handleCall(any(), any(), any())).thenReturn(responseBody);

        BytesMessage responseMessage = mock(BytesMessage.class);
        when(session.createBytesMessage()).thenReturn(responseMessage);

        // when
        sut.start();
        Thread.sleep(10);

        // then
        verify(session, times(1)).createProducer(replyQueue);
        verify(session, times(1)).createConsumer(requestQueue);
        verify(session, atLeast(1)).createBytesMessage();
        verify(messageConsumer, atLeast(1)).receive();
        verify(message, atLeast(1)).getJMSType();
        verify(message, atLeast(1)).getJMSCorrelationID();
        verify(message, atLeast(1)).getPropertyNames();
        verify(message, atLeast(1)).readBytes(requestBody.get());
        verify(handler, atLeast(1)).handleCall(any(), any(), any());
        verify(responseMessage, atLeast(1)).writeBytes(responseBody);

        Throwable e = Asserts.assertThrows(sut::start);
        Asserts.assertEqualsType(e, java.lang.IllegalStateException.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void handlFireTest() throws Exception {
        // given
        Destination requestQueue = mock(Destination.class);
        Destination replyQueue = mock(Destination.class);
        int threadPoolSize = 1;

        Session session = mock(Session.class);
        MessageProducer messageProducer = mock(MessageProducer.class);
        when(session.createProducer(replyQueue)).thenReturn(messageProducer);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        when(session.createConsumer(requestQueue)).thenReturn(messageConsumer);

        EzyActiveRpcCallHandler handler = mock(EzyActiveRpcCallHandler.class);

        EzyActiveRpcServer sut = EzyActiveRpcServer.builder()
            .session(session)
            .threadPoolSize(threadPoolSize)
            .requestQueue(requestQueue)
            .replyQueue(replyQueue)
            .build();
        sut.setCallHandler(handler);

        BytesMessage message = mock(BytesMessage.class);
        when(messageConsumer.receive()).thenReturn(message);

        BytesMessage bytesMessage = mock(BytesMessage.class);
        when(session.createBytesMessage()).thenReturn(bytesMessage);

        String messageType = RandomUtil.randomShortAlphabetString();
        when(message.getJMSType()).thenReturn(messageType);

        Enumeration<String> propertyNames = mock(Enumeration.class);
        when(message.getPropertyNames()).thenReturn(propertyNames);

        AtomicReference<byte[]> requestBody = new AtomicReference<>();
        when(message.readBytes(any(byte[].class))).thenAnswer(it -> {
            if (it.getArguments().length > 0) {
                requestBody.set(it.getArgumentAt(0, byte[].class));
            }
            return 0;
        });

        byte[] responseBody = RandomUtil.randomShortByteArray();
        when(handler.handleCall(any(), any(), any())).thenReturn(responseBody);

        // when
        sut.start();
        Thread.sleep(10);

        // then
        verify(session, times(1)).createProducer(replyQueue);
        verify(session, times(1)).createConsumer(requestQueue);
        verify(session, times(0)).createBytesMessage();
        verify(messageConsumer, atLeast(1)).receive();
        verify(message, atLeast(1)).getJMSType();
        verify(message, atLeast(1)).getJMSCorrelationID();
        verify(message, atLeast(1)).getPropertyNames();
        verify(message, atLeast(1)).readBytes(requestBody.get());
        verify(handler, times(0)).handleCall(any(), any(), any());
        verify(handler, atLeast(1)).handleFire(any(), any());

        Throwable e = Asserts.assertThrows(sut::start);
        Asserts.assertEqualsType(e, java.lang.IllegalStateException.class);
    }
}
