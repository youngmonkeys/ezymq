package com.tvd12.ezymq.mosquitto.test.endpoint;

import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoMessage;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoRpcClient;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallback;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.exception.EzyMosquittoMaxCapacity;
import com.tvd12.ezymq.mosquitto.exception.EzyMqttConnectionLostException;
import com.tvd12.ezymq.mosquitto.factory.EzyMosquittoCorrelationIdFactory;
import com.tvd12.ezymq.mosquitto.factory.EzyMosquittoSimpleCorrelationIdFactory;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoResponseConsumer;
import com.tvd12.ezymq.mosquitto.message.EzyMqttMqMessage;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

public class EzyMosquittoRpcClientTest extends BaseTest {

    @Test
    public void doFireTest() throws Exception {
        // given
        int capacity = RandomUtil.randomInt(10, 10000);
        int defaultTimeout = 3 * 1000;

        EzyMosquittoResponseConsumer unconsumedResponseConsumer =
            mock(EzyMosquittoResponseConsumer.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);

        EzyMosquittoRpcClient sut = EzyMosquittoRpcClient.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(replyTopic)
            .capacity(capacity)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(
                new EzyMosquittoSimpleCorrelationIdFactory()
            )
            .unconsumedResponseConsumer(unconsumedResponseConsumer)
            .build();

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

        // when
        sut.doFire(properties, message);

        // then
        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verify(mqttClient, times(1)).publish(
            anyString(),
            any(EzyMqttMqMessage.class)
        );
        verifyNoMoreInteractions(mqttClient);

        sut.close();
    }

    @Test
    public void doFireWithPropertiesIsNullTest() throws Exception {
        // given
        int capacity = RandomUtil.randomInt(10, 10000);
        int defaultTimeout = 3 * 1000;

        EzyMosquittoResponseConsumer unconsumedResponseConsumer =
            mock(EzyMosquittoResponseConsumer.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);

        EzyMosquittoRpcClient sut = EzyMosquittoRpcClient.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(replyTopic)
            .capacity(capacity)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(
                new EzyMosquittoSimpleCorrelationIdFactory()
            )
            .unconsumedResponseConsumer(unconsumedResponseConsumer)
            .build();

        byte[] message = RandomUtil.randomShortByteArray();

        // when
        sut.doFire(null, message);

        // then
        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verify(mqttClient, times(1)).publish(
            anyString(),
            any(EzyMqttMqMessage.class)
        );
        verifyNoMoreInteractions(mqttClient);

        sut.close();
    }

    @Test
    public void doCallTest() throws Exception {
        // given
        int capacity = RandomUtil.randomInt(10, 10000);
        int defaultTimeout = 100;

        EzyMosquittoResponseConsumer unconsumedResponseConsumer =
            mock(EzyMosquittoResponseConsumer.class);

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();

        AtomicReference<EzyMqttCallback> callbackRef = new AtomicReference<>();
        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);
        doAnswer(it -> {
            callbackRef.set(it.getArgumentAt(1, EzyMqttCallback.class));
            return null;
        }).when(mqttClient).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );

        EzyMosquittoCorrelationIdFactory correlationIdFactory
            = mock(EzyMosquittoCorrelationIdFactory.class);
        String requestId = RandomUtil.randomShortAlphabetString();
        when(correlationIdFactory.newCorrelationId(topic)).thenReturn(requestId);

        EzyMosquittoRpcClient sut = EzyMosquittoRpcClient.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(replyTopic)
            .capacity(capacity)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .unconsumedResponseConsumer(unconsumedResponseConsumer)
            .build();

        byte[] message = RandomUtil.randomShortByteArray();

        // when
        AtomicReference<EzyMosquittoMessage> resultRef = new AtomicReference<>();
        new Thread(() -> {
            try {
                resultRef.set(sut.doCall(null, message));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).start();
        Thread.sleep(20);
        callbackRef.get().messageArrived(
            EzyMosquittoProperties.builder()
                .correlationId(requestId)
                .build(),
            message
        );
        Thread.sleep(100);

        // then
        EzyMosquittoMessage expectedMessage = new EzyMosquittoMessage(
            EzyMosquittoProperties.builder()
                .correlationId(requestId)
                .build(),
            message
        );
        Asserts.assertEquals(resultRef.get(), expectedMessage);
        verify(correlationIdFactory, times(1)).newCorrelationId(topic);
        verifyNoMoreInteractions(correlationIdFactory);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verify(mqttClient, times(1)).publish(
            anyString(),
            any(EzyMqttMqMessage.class)
        );
        verifyNoMoreInteractions(mqttClient);
        sut.close();
    }

    @Test
    public void doCallWithUnconsumedResponseConsumerTest() throws Exception {
        // given
        int capacity = RandomUtil.randomInt(10, 10000);
        int defaultTimeout = 3 * 1000;

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();

        AtomicReference<EzyMqttCallback> callbackRef = new AtomicReference<>();
        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);
        doAnswer(it -> {
            callbackRef.set(it.getArgumentAt(1, EzyMqttCallback.class));
            return null;
        }).when(mqttClient).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );

        EzyMosquittoCorrelationIdFactory correlationIdFactory
            = mock(EzyMosquittoCorrelationIdFactory.class);
        String requestId = RandomUtil.randomShortAlphabetString();
        when(correlationIdFactory.newCorrelationId(topic)).thenReturn(requestId);

        EzyMosquittoResponseConsumer unconsumedResponseConsumer = mock(
            EzyMosquittoResponseConsumer.class
        );

        EzyMosquittoRpcClient sut = EzyMosquittoRpcClient.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(replyTopic)
            .capacity(capacity)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .unconsumedResponseConsumer(unconsumedResponseConsumer)
            .build();

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .correlationId(requestId)
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

        // when
        callbackRef.get().messageArrived(
            properties,
            message
        );

        // then
        verifyNoMoreInteractions(correlationIdFactory);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verifyNoMoreInteractions(mqttClient);

        verify(unconsumedResponseConsumer, times(1)).consume(
            properties,
            message
        );
        verifyNoMoreInteractions(unconsumedResponseConsumer);
        sut.close();
    }

    @Test
    public void doCallWithoutUnconsumedResponseConsumerTest() throws Exception {
        // given
        int capacity = RandomUtil.randomInt(10, 10000);
        int defaultTimeout = 3 * 1000;

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();

        AtomicReference<EzyMqttCallback> callbackRef = new AtomicReference<>();
        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);
        doAnswer(it -> {
            callbackRef.set(it.getArgumentAt(1, EzyMqttCallback.class));
            return null;
        }).when(mqttClient).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );

        EzyMosquittoCorrelationIdFactory correlationIdFactory
            = mock(EzyMosquittoCorrelationIdFactory.class);
        String requestId = RandomUtil.randomShortAlphabetString();
        when(correlationIdFactory.newCorrelationId(topic)).thenReturn(requestId);

        EzyMosquittoRpcClient sut = EzyMosquittoRpcClient.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(replyTopic)
            .capacity(capacity)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .build();

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .correlationId(requestId)
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

        // when
        callbackRef.get().messageArrived(
            properties,
            message
        );

        // then
        verifyNoMoreInteractions(correlationIdFactory);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verifyNoMoreInteractions(mqttClient);
        sut.close();
    }

    @Test
    public void doCallFailedTest() throws Exception {
        // given
        int capacity = RandomUtil.randomInt(10, 10000);
        int defaultTimeout = 3 * 1000;

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();

        AtomicReference<EzyMqttCallback> callbackRef = new AtomicReference<>();
        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);
        doAnswer(it -> {
            callbackRef.set(it.getArgumentAt(1, EzyMqttCallback.class));
            return null;
        }).when(mqttClient).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );

        EzyMosquittoCorrelationIdFactory correlationIdFactory
            = mock(EzyMosquittoCorrelationIdFactory.class);
        String requestId = RandomUtil.randomShortAlphabetString();
        when(correlationIdFactory.newCorrelationId(topic)).thenReturn(requestId);

        EzyMosquittoRpcClient sut = EzyMosquittoRpcClient.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(replyTopic)
            .capacity(capacity)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .build();

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

        // when
        AtomicReference<Exception> resultRef = new AtomicReference<>();
        new Thread(() -> {
            try {
                sut.doCall(properties, message);
            } catch (Exception e) {
                resultRef.set(e);
            }
        }).start();
        Thread.sleep(50);
        Exception error = new Exception("test");
        EzyMqttConnectionLostException exception = new EzyMqttConnectionLostException(error);
        callbackRef.get().connectionLost(exception);
        Thread.sleep(100);

        // then
        Asserts.assertEquals(resultRef.get(), exception);

        verify(correlationIdFactory, times(1)).newCorrelationId(topic);
        verifyNoMoreInteractions(correlationIdFactory);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verify(mqttClient, times(1)).publish(
            anyString(),
            any(EzyMqttMqMessage.class)
        );
        verifyNoMoreInteractions(mqttClient);
        sut.close();
    }

    @Test
    public void doCallDueToTimeout() throws Exception {
        // given
        int capacity = RandomUtil.randomInt(10, 10000);
        int defaultTimeout = 100;

        String topic = RandomUtil.randomShortAlphabetString();

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);

        EzyMosquittoCorrelationIdFactory correlationIdFactory
            = mock(EzyMosquittoCorrelationIdFactory.class);
        String requestId = RandomUtil.randomShortAlphabetString();
        when(correlationIdFactory.newCorrelationId(topic)).thenReturn(requestId);

        EzyMosquittoRpcClient sut = EzyMosquittoRpcClient.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .capacity(capacity)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .build();

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

        // when
        Throwable e = Asserts.assertThrows(() ->
            sut.doCall(properties, message, 10)
        );
        Thread.sleep(50);

        // then
        Asserts.assertEqualsType(e, TimeoutException.class);
        verify(correlationIdFactory, times(1)).newCorrelationId(topic);
        verifyNoMoreInteractions(correlationIdFactory);

        verify(mqttClient, times(1)).publish(
            anyString(),
            any(EzyMqttMqMessage.class)
        );
        verifyNoMoreInteractions(mqttClient);
        sut.close();
    }

    @Test
    public void doCallDueToMaxCapacity() throws Exception {
        // given
        int capacity = 1;
        int defaultTimeout = 3 * 1000;

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);

        EzyMosquittoCorrelationIdFactory correlationIdFactory
            = mock(EzyMosquittoCorrelationIdFactory.class);
        String requestId = RandomUtil.randomShortAlphabetString();
        when(correlationIdFactory.newCorrelationId(topic)).thenReturn(requestId);

        EzyMosquittoRpcClient sut = EzyMosquittoRpcClient.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(replyTopic)
            .capacity(capacity)
            .defaultTimeout(defaultTimeout)
            .correlationIdFactory(correlationIdFactory)
            .build();

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

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
        Asserts.assertEqualsType(e, EzyMosquittoMaxCapacity.class);
        verify(correlationIdFactory, times(1)).newCorrelationId(topic);
        verifyNoMoreInteractions(correlationIdFactory);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verify(mqttClient, times(1)).publish(
            anyString(),
            any(EzyMqttMqMessage.class)
        );
        verifyNoMoreInteractions(mqttClient);
        sut.close();
    }

    @Test
    public void buildWithDefaultCorrelationIdFactory() {
        // given
        int capacity = RandomUtil.randomInt(10, 10000);
        int defaultTimeout = 3 * 1000;

        String topic = RandomUtil.randomShortAlphabetString();
        String replyTopic = RandomUtil.randomShortAlphabetString();

        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);

        // when
        EzyMosquittoRpcClient sut = EzyMosquittoRpcClient.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(replyTopic)
            .capacity(capacity)
            .defaultTimeout(defaultTimeout)
            .build();

        // then
        sut.close();
    }
}
