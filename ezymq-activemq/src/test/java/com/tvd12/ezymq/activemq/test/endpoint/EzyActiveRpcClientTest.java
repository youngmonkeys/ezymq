package com.tvd12.ezymq.activemq.test.endpoint;

import com.tvd12.ezyfox.util.EzyThreads;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveMessage;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveRpcClient;
import com.tvd12.ezymq.activemq.exception.EzyActiveMaxCapacity;
import com.tvd12.ezymq.activemq.factory.EzyActiveCorrelationIdFactory;
import com.tvd12.ezymq.activemq.factory.EzyActiveSimpleCorrelationIdFactory;
import com.tvd12.ezymq.activemq.handler.EzyActiveResponseConsumer;
import com.tvd12.ezymq.activemq.util.EzyActiveProperties;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.jms.*;
import java.util.Enumeration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

public class EzyActiveRpcClientTest extends BaseTest {

    @Test
    public void doFireTest() throws Exception {
        // given
        Destination requestQueue = mock(Destination.class);
        Destination replyQueue = mock(Destination.class);
        int capacity = RandomUtil.randomInt(10, 10000);
        int threadPoolSize = 1;
        int defaultTimeout = 3 * 1000;

        EzyActiveResponseConsumer unconsumedResponseConsumer =
            mock(EzyActiveResponseConsumer.class);

        Session session = mock(Session.class);
        MessageProducer messageProducer = mock(MessageProducer.class);
        when(session.createProducer(requestQueue)).thenReturn(messageProducer);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        when(session.createConsumer(replyQueue)).thenReturn(messageConsumer);

        EzyActiveRpcClient sut = EzyActiveRpcClient.builder()
            .session(session)
            .capacity(capacity)
            .threadPoolSize(threadPoolSize)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(
                new EzyActiveSimpleCorrelationIdFactory()
            )
            .requestQueue(requestQueue)
            .replyQueue(replyQueue)
            .unconsumedResponseConsumer(unconsumedResponseConsumer)
            .build();

        EzyActiveProperties properties = EzyActiveProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

        BytesMessage bytesMessage = mock(BytesMessage.class);
        when(session.createBytesMessage()).thenReturn(bytesMessage);

        // when
        sut.doFire(properties, message);

        // then
        verify(session, times(1)).createProducer(requestQueue);
        verify(session, times(1)).createConsumer(replyQueue);
        verify(session, times(1)).createBytesMessage();
        verify(bytesMessage, times(1)).writeBytes(message);
        verify(messageProducer, times(1)).send(bytesMessage);
        sut.close();
    }

    @Test
    public void doFireWithPropertiesIsNullTest() throws Exception {
        // given
        Destination requestQueue = mock(Destination.class);
        Destination replyQueue = mock(Destination.class);
        int capacity = RandomUtil.randomInt(10, 10000);
        int threadPoolSize = 1;
        int defaultTimeout = 3 * 1000;

        EzyActiveResponseConsumer unconsumedResponseConsumer =
            mock(EzyActiveResponseConsumer.class);

        Session session = mock(Session.class);
        MessageProducer messageProducer = mock(MessageProducer.class);
        when(session.createProducer(requestQueue)).thenReturn(messageProducer);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        when(session.createConsumer(replyQueue)).thenReturn(messageConsumer);

        EzyActiveRpcClient sut = EzyActiveRpcClient.builder()
            .session(session)
            .capacity(capacity)
            .threadPoolSize(threadPoolSize)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(
                new EzyActiveSimpleCorrelationIdFactory()
            )
            .requestQueue(requestQueue)
            .replyQueue(replyQueue)
            .unconsumedResponseConsumer(unconsumedResponseConsumer)
            .build();

        byte[] message = RandomUtil.randomShortByteArray();

        BytesMessage bytesMessage = mock(BytesMessage.class);
        when(session.createBytesMessage()).thenReturn(bytesMessage);

        // when
        sut.doFire(null, message);

        // then
        verify(session, times(1)).createProducer(requestQueue);
        verify(session, times(1)).createConsumer(replyQueue);
        verify(session, times(1)).createBytesMessage();
        verify(bytesMessage, times(1)).writeBytes(message);
        verify(messageProducer, times(1)).send(bytesMessage);
        sut.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doCallTest() throws Exception {
        // given
        Destination requestQueue = mock(Destination.class);
        Destination replyQueue = mock(Destination.class);
        int capacity = RandomUtil.randomInt(10, 10000);
        int threadPoolSize = 1;
        int defaultTimeout = 100;

        EzyActiveResponseConsumer unconsumedResponseConsumer =
            mock(EzyActiveResponseConsumer.class);

        Session session = mock(Session.class);
        MessageProducer messageProducer = mock(MessageProducer.class);
        when(session.createProducer(requestQueue)).thenReturn(messageProducer);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        when(session.createConsumer(replyQueue)).thenReturn(messageConsumer);

        EzyActiveCorrelationIdFactory correlationIdFactory
            = mock(EzyActiveCorrelationIdFactory.class);
        String requestId = RandomUtil.randomShortAlphabetString();
        when(correlationIdFactory.newCorrelationId()).thenReturn(requestId);

        BytesMessage responseByteMessage = mock(BytesMessage.class);
        when(messageConsumer.receive()).thenAnswer(it -> {
            EzyThreads.sleep(10);
            return responseByteMessage;
        });

        AtomicReference<byte[]> responseBody = new AtomicReference<>();
        when(responseByteMessage.readBytes(any(byte[].class))).thenAnswer(it -> {
            if (it.getArguments().length > 0) {
                responseBody.set(it.getArgumentAt(0, byte[].class));
            }
            return 0;
        });

        when(responseByteMessage.getJMSCorrelationID()).thenReturn(requestId);

        Enumeration<String> propertyNames = mock(Enumeration.class);
        when(responseByteMessage.getPropertyNames()).thenReturn(propertyNames);

        String jmsType = RandomUtil.randomShortAlphabetString();
        when(responseByteMessage.getJMSType()).thenReturn(jmsType);

        EzyActiveRpcClient sut = EzyActiveRpcClient.builder()
            .session(session)
            .capacity(capacity)
            .threadPoolSize(threadPoolSize)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .requestQueue(requestQueue)
            .replyQueue(replyQueue)
            .unconsumedResponseConsumer(unconsumedResponseConsumer)
            .build();

        EzyActiveProperties properties = EzyActiveProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

        BytesMessage bytesMessage = mock(BytesMessage.class);
        when(session.createBytesMessage()).thenReturn(bytesMessage);

        // when
        EzyActiveMessage result = sut.doCall(properties, message);
        Thread.sleep(50);

        // then
        EzyActiveMessage expectedMessage = new EzyActiveMessage(
            EzyActiveProperties.builder()
                .type(jmsType)
                .correlationId(requestId)
                .build(),
            responseBody.get()
        );
        Asserts.assertEquals(result, expectedMessage);
        verify(session, times(1)).createProducer(requestQueue);
        verify(session, times(1)).createConsumer(replyQueue);
        verify(session, times(1)).createBytesMessage();
        verify(correlationIdFactory, times(1)).newCorrelationId();
        verify(bytesMessage, times(1)).writeBytes(message);
        verify(messageProducer, times(1)).send(bytesMessage);
        verify(messageConsumer, atLeast(1)).receive();
        verify(responseByteMessage, atLeast(1)).getJMSCorrelationID();
        verify(responseByteMessage, atLeast(1)).getPropertyNames();
        verify(responseByteMessage, atLeast(1)).readBytes(any());
        verify(responseByteMessage, atLeast(1)).getJMSType();
        verify(unconsumedResponseConsumer, atLeast(1)).consume(
            any(),
            any()
        );
        sut.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doCallWithoutUnconsumedResponseConsumerTest() throws Exception {
        // given
        Destination requestQueue = mock(Destination.class);
        Destination replyQueue = mock(Destination.class);
        int capacity = RandomUtil.randomInt(10, 10000);
        int threadPoolSize = 1;
        int defaultTimeout = 3 * 1000;

        Session session = mock(Session.class);
        MessageProducer messageProducer = mock(MessageProducer.class);
        when(session.createProducer(requestQueue)).thenReturn(messageProducer);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        when(session.createConsumer(replyQueue)).thenReturn(messageConsumer);

        EzyActiveCorrelationIdFactory correlationIdFactory
            = mock(EzyActiveCorrelationIdFactory.class);
        String requestId = RandomUtil.randomShortAlphabetString();
        when(correlationIdFactory.newCorrelationId()).thenReturn(requestId);

        BytesMessage responseByteMessage = mock(BytesMessage.class);
        when(messageConsumer.receive()).thenAnswer(it -> {
            EzyThreads.sleep(10);
            return responseByteMessage;
        });

        AtomicReference<byte[]> responseBody = new AtomicReference<>();
        when(responseByteMessage.readBytes(any(byte[].class))).thenAnswer(it -> {
            if (it.getArguments().length > 0) {
                responseBody.set(it.getArgumentAt(0, byte[].class));
            }
            return 0;
        });

        when(responseByteMessage.getJMSCorrelationID()).thenReturn(requestId);

        Enumeration<String> propertyNames = mock(Enumeration.class);
        when(responseByteMessage.getPropertyNames()).thenReturn(propertyNames);

        String jmsType = RandomUtil.randomShortAlphabetString();
        when(responseByteMessage.getJMSType()).thenReturn(jmsType);

        EzyActiveRpcClient sut = EzyActiveRpcClient.builder()
            .session(session)
            .capacity(capacity)
            .threadPoolSize(threadPoolSize)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .requestQueue(requestQueue)
            .replyQueue(replyQueue)
            .build();

        EzyActiveProperties properties = EzyActiveProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

        BytesMessage bytesMessage = mock(BytesMessage.class);
        when(session.createBytesMessage()).thenReturn(bytesMessage);

        // when
        EzyActiveMessage result = sut.doCall(properties, message);
        Thread.sleep(50);

        // then
        Asserts.assertEquals(
            result,
            new EzyActiveMessage(
                EzyActiveProperties.builder()
                    .type(jmsType)
                    .correlationId(requestId)
                    .build(),
                responseBody.get()
            )
        );
        verify(session, times(1)).createProducer(requestQueue);
        verify(session, times(1)).createConsumer(replyQueue);
        verify(session, times(1)).createBytesMessage();
        verify(bytesMessage, times(1)).writeBytes(message);
        verify(messageProducer, times(1)).send(bytesMessage);
        verify(messageConsumer, atLeast(1)).receive();
        verify(responseByteMessage, atLeast(1)).getJMSCorrelationID();
        verify(responseByteMessage, atLeast(1)).getPropertyNames();
        verify(responseByteMessage, atLeast(1)).readBytes(any());
        verify(responseByteMessage, atLeast(1)).getJMSType();
        verify(correlationIdFactory, times(1)).newCorrelationId();
        sut.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doCallFailedTest() throws Exception {
        // given
        Destination requestQueue = mock(Destination.class);
        Destination replyQueue = mock(Destination.class);
        int capacity = RandomUtil.randomInt(10, 10000);
        int threadPoolSize = 1;
        int defaultTimeout = 3 * 1000;

        Session session = mock(Session.class);
        MessageProducer messageProducer = mock(MessageProducer.class);
        when(session.createProducer(requestQueue)).thenReturn(messageProducer);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        when(session.createConsumer(replyQueue)).thenReturn(messageConsumer);

        EzyActiveCorrelationIdFactory correlationIdFactory
            = mock(EzyActiveCorrelationIdFactory.class);
        String requestId = RandomUtil.randomShortAlphabetString();
        when(correlationIdFactory.newCorrelationId()).thenReturn(requestId);

        BytesMessage responseByteMessage = mock(BytesMessage.class);
        when(messageConsumer.receive()).thenAnswer(it -> {
            EzyThreads.sleep(10);
            return responseByteMessage;
        });

        RuntimeException exception = new RuntimeException("test");
        when(responseByteMessage.readBytes(any(byte[].class))).thenAnswer(it -> {
            if (it.getArguments().length > 0) {
                throw exception;
            }
            return 0;
        });

        when(responseByteMessage.getJMSCorrelationID()).thenReturn(requestId);

        Enumeration<String> propertyNames = mock(Enumeration.class);
        when(responseByteMessage.getPropertyNames()).thenReturn(propertyNames);

        String jmsType = RandomUtil.randomShortAlphabetString();
        when(responseByteMessage.getJMSType()).thenReturn(jmsType);

        EzyActiveRpcClient sut = EzyActiveRpcClient.builder()
            .session(session)
            .capacity(capacity)
            .threadPoolSize(threadPoolSize)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .requestQueue(requestQueue)
            .replyQueue(replyQueue)
            .build();

        EzyActiveProperties properties = EzyActiveProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

        BytesMessage bytesMessage = mock(BytesMessage.class);
        when(session.createBytesMessage()).thenReturn(bytesMessage);

        // when
        Throwable e = Asserts.assertThrows(() ->
            sut.doCall(properties, message)
        );
        Thread.sleep(50);

        // then
        Asserts.assertEquals(e, exception);
        verify(session, times(1)).createProducer(requestQueue);
        verify(session, times(1)).createConsumer(replyQueue);
        verify(session, times(1)).createBytesMessage();
        verify(bytesMessage, times(1)).writeBytes(message);
        verify(messageProducer, times(1)).send(bytesMessage);
        verify(messageConsumer, atLeast(1)).receive();
        verify(responseByteMessage, atLeast(1)).getJMSCorrelationID();
        verify(responseByteMessage, atLeast(1)).getPropertyNames();
        verify(responseByteMessage, atLeast(1)).readBytes(any());
        verify(responseByteMessage, atLeast(1)).getJMSType();
        verify(correlationIdFactory, times(1)).newCorrelationId();
        sut.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doCallDueToTimeout() throws Exception {
        // given
        Destination requestQueue = mock(Destination.class);
        Destination replyQueue = mock(Destination.class);
        int capacity = RandomUtil.randomInt(10, 10000);
        int threadPoolSize = 1;
        int defaultTimeout = 3 * 1000;

        Session session = mock(Session.class);
        MessageProducer messageProducer = mock(MessageProducer.class);
        when(session.createProducer(requestQueue)).thenReturn(messageProducer);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        when(session.createConsumer(replyQueue)).thenReturn(messageConsumer);

        EzyActiveCorrelationIdFactory correlationIdFactory
            = mock(EzyActiveCorrelationIdFactory.class);
        String requestId = RandomUtil.randomShortAlphabetString();
        when(correlationIdFactory.newCorrelationId()).thenReturn(requestId);

        BytesMessage responseByteMessage = mock(BytesMessage.class);
        when(messageConsumer.receive()).thenAnswer(it -> {
            EzyThreads.sleep(10);
            return responseByteMessage;
        });

        when(responseByteMessage.readBytes(any(byte[].class))).thenAnswer(it -> {
            if (it.getArguments().length > 0) {
                EzyThreads.sleep(100);
            }
            return 0;
        });

        when(responseByteMessage.getJMSCorrelationID()).thenReturn(requestId);

        Enumeration<String> propertyNames = mock(Enumeration.class);
        when(responseByteMessage.getPropertyNames()).thenReturn(propertyNames);

        String jmsType = RandomUtil.randomShortAlphabetString();
        when(responseByteMessage.getJMSType()).thenReturn(jmsType);

        EzyActiveRpcClient sut = EzyActiveRpcClient.builder()
            .session(session)
            .capacity(capacity)
            .threadPoolSize(threadPoolSize)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .requestQueue(requestQueue)
            .replyQueue(replyQueue)
            .build();

        EzyActiveProperties properties = EzyActiveProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

        BytesMessage bytesMessage = mock(BytesMessage.class);
        when(session.createBytesMessage()).thenReturn(bytesMessage);

        // when
        Throwable e = Asserts.assertThrows(() ->
            sut.doCall(properties, message, 10)
        );
        Thread.sleep(50);

        // then
        Asserts.assertEqualsType(e, TimeoutException.class);
        verify(session, times(1)).createProducer(requestQueue);
        verify(session, times(1)).createConsumer(replyQueue);
        verify(session, times(1)).createBytesMessage();
        verify(bytesMessage, times(1)).writeBytes(message);
        verify(messageProducer, times(1)).send(bytesMessage);
        verify(messageConsumer, atLeast(1)).receive();
        verify(responseByteMessage, atLeast(1)).getJMSCorrelationID();
        verify(responseByteMessage, atLeast(1)).getPropertyNames();
        verify(responseByteMessage, atLeast(1)).readBytes(any());
        verify(responseByteMessage, atLeast(1)).getJMSType();
        verify(correlationIdFactory, times(1)).newCorrelationId();
        sut.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void doCallDueToMaxCapacity() throws Exception {
        // given
        Destination requestQueue = mock(Destination.class);
        Destination replyQueue = mock(Destination.class);
        int capacity = 1;
        int threadPoolSize = 1;
        int defaultTimeout = 3 * 1000;

        Session session = mock(Session.class);
        MessageProducer messageProducer = mock(MessageProducer.class);
        when(session.createProducer(requestQueue)).thenReturn(messageProducer);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        when(session.createConsumer(replyQueue)).thenReturn(messageConsumer);

        EzyActiveCorrelationIdFactory correlationIdFactory
            = mock(EzyActiveCorrelationIdFactory.class);
        String requestId = RandomUtil.randomShortAlphabetString();
        when(correlationIdFactory.newCorrelationId()).thenReturn(requestId);

        BytesMessage responseByteMessage = mock(BytesMessage.class);
        when(messageConsumer.receive()).thenAnswer(it -> responseByteMessage);

        when(responseByteMessage.readBytes(any(byte[].class))).thenAnswer(it -> {
            if (it.getArguments().length > 0) {
                EzyThreads.sleep(100);
            }
            return 0;
        });

        when(responseByteMessage.getJMSCorrelationID()).thenReturn(requestId);

        Enumeration<String> propertyNames = mock(Enumeration.class);
        when(responseByteMessage.getPropertyNames()).thenReturn(propertyNames);

        String jmsType = RandomUtil.randomShortAlphabetString();
        when(responseByteMessage.getJMSType()).thenReturn(jmsType);

        EzyActiveRpcClient sut = EzyActiveRpcClient.builder()
            .session(session)
            .capacity(capacity)
            .threadPoolSize(threadPoolSize)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .requestQueue(requestQueue)
            .replyQueue(replyQueue)
            .build();

        EzyActiveProperties properties = EzyActiveProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

        BytesMessage bytesMessage = mock(BytesMessage.class);
        when(session.createBytesMessage()).thenReturn(bytesMessage);

        // when
        new Thread(() -> {
            try {
                sut.doCall(properties, message);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        Thread.sleep(10);
        Throwable e = Asserts.assertThrows(() ->
            sut.doCall(properties, message)
        );
        Thread.sleep(10);

        // then
        Asserts.assertEqualsType(e, EzyActiveMaxCapacity.class);
        verify(session, times(1)).createProducer(requestQueue);
        verify(session, times(1)).createConsumer(replyQueue);
        verify(session, times(1)).createBytesMessage();
        verify(bytesMessage, times(1)).writeBytes(message);
        verify(messageProducer, times(1)).send(bytesMessage);
        verify(messageConsumer, atLeast(1)).receive();
        verify(responseByteMessage, atLeast(1)).getJMSCorrelationID();
        verify(responseByteMessage, atLeast(1)).getPropertyNames();
        verify(responseByteMessage, atLeast(1)).readBytes(any());
        verify(responseByteMessage, atLeast(1)).getJMSType();
        verify(correlationIdFactory, times(1)).newCorrelationId();
        sut.close();
    }

    @Test
    public void buildWithDefaultCorrelationIdFactory() throws Exception {
        // given
        Destination requestQueue = mock(Destination.class);
        Destination replyQueue = mock(Destination.class);
        int capacity = RandomUtil.randomInt(10, 10000);
        int threadPoolSize = 1;
        int defaultTimeout = 3 * 1000;

        Session session = mock(Session.class);
        MessageProducer messageProducer = mock(MessageProducer.class);
        when(session.createProducer(requestQueue)).thenReturn(messageProducer);

        MessageConsumer messageConsumer = mock(MessageConsumer.class);
        when(session.createConsumer(replyQueue)).thenReturn(messageConsumer);

        // when
        EzyActiveRpcClient sut = EzyActiveRpcClient.builder()
            .session(session)
            .capacity(capacity)
            .threadPoolSize(threadPoolSize)
            .defaultTimeout(defaultTimeout)
            .requestQueue(requestQueue)
            .replyQueue(replyQueue)
            .build();

        // then
        verify(session, times(1)).createProducer(requestQueue);
        verify(session, times(1)).createConsumer(replyQueue);
        sut.close();
    }
}
