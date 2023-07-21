package com.tvd12.ezymq.mosquitto.test.endpoint;

import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoRpcServer;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallback;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.exception.EzyMqttConnectionLostException;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoRpcCallHandler;
import com.tvd12.ezymq.mosquitto.message.EzyMqttMqMessage;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

public class EzyMosquittoRpcServerTest extends BaseTest {

    @Test
    public void startTest() throws Exception {
        // given
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

        EzyMosquittoRpcCallHandler handler = mock(EzyMosquittoRpcCallHandler.class);

        EzyMosquittoRpcServer sut = EzyMosquittoRpcServer.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(replyTopic)
            .build();
        sut.setCallHandler(handler);

        // when
        new Thread(sut::start).start();
        Thread.sleep(50);

        String messageType = RandomUtil.randomShortAlphabetString();
        String requestId = RandomUtil.randomShortAlphabetString();

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .correlationId(requestId)
            .messageType(messageType)
            .build();
        byte[] body = RandomUtil.randomShortByteArray();
        callbackRef.get().messageArrived(properties, body);
        Thread.sleep(100);

        // then
        verify(handler, atLeast(1)).handleCall(any(), any());
        verifyNoMoreInteractions(handler);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verify(mqttClient, times(1)).publish(
            anyString(),
            any(EzyMqttMqMessage.class)
        );
        verifyNoMoreInteractions(mqttClient);

        Throwable e = Asserts.assertThrows(sut::start);
        Asserts.assertEqualsType(e, IllegalStateException.class);
        sut.close();
        callbackRef.get().connectionLost(
            new EzyMqttConnectionLostException(new RuntimeException("test"))
        );
    }

    @Test
    public void handleFireDueToCorrelationIdNullTest() throws Exception {
        // given
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

        EzyMosquittoRpcCallHandler handler = mock(EzyMosquittoRpcCallHandler.class);

        EzyMosquittoRpcServer sut = EzyMosquittoRpcServer.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(replyTopic)
            .build();
        sut.setCallHandler(handler);

        // when
        new Thread(sut::start).start();
        Thread.sleep(50);

        String messageType = RandomUtil.randomShortAlphabetString();

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .messageType(messageType)
            .build();
        byte[] body = RandomUtil.randomShortByteArray();
        callbackRef.get().messageArrived(properties, body);
        Thread.sleep(100);

        // then
        verify(handler, atLeast(1)).handleFire(any());
        verifyNoMoreInteractions(handler);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verifyNoMoreInteractions(mqttClient);
        sut.close();
    }

    @Test
    public void handleFireDueToReplyTopicNullTest() throws Exception {
        // given
        String topic = RandomUtil.randomShortAlphabetString();

        AtomicReference<EzyMqttCallback> callbackRef = new AtomicReference<>();
        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);
        doAnswer(it -> {
            callbackRef.set(it.getArgumentAt(1, EzyMqttCallback.class));
            return null;
        }).when(mqttClient).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );

        EzyMosquittoRpcCallHandler handler = mock(EzyMosquittoRpcCallHandler.class);

        EzyMosquittoRpcServer sut = EzyMosquittoRpcServer.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(null)
            .build();
        sut.setCallHandler(handler);

        // when
        new Thread(sut::start).start();
        Thread.sleep(50);

        String messageType = RandomUtil.randomShortAlphabetString();

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .messageType(messageType)
            .build();
        byte[] body = RandomUtil.randomShortByteArray();
        callbackRef.get().messageArrived(properties, body);
        Thread.sleep(100);

        // then
        verify(handler, atLeast(1)).handleFire(any());
        verifyNoMoreInteractions(handler);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verifyNoMoreInteractions(mqttClient);
        sut.close();
    }

    @Test
    public void handleFireThrowCancellationExceptionTest() throws Exception {
        // given
        String topic = RandomUtil.randomShortAlphabetString();

        AtomicReference<EzyMqttCallback> callbackRef = new AtomicReference<>();
        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);
        doAnswer(it -> {
            callbackRef.set(it.getArgumentAt(1, EzyMqttCallback.class));
            return null;
        }).when(mqttClient).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );

        EzyMosquittoRpcCallHandler handler = mock(EzyMosquittoRpcCallHandler.class);
        CancellationException exception = new CancellationException();
        doThrow(exception).when(handler).handleFire(any());

        EzyMosquittoRpcServer sut = EzyMosquittoRpcServer.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(null)
            .build();
        sut.setCallHandler(handler);

        // when
        new Thread(sut::start).start();
        Thread.sleep(50);

        String messageType = RandomUtil.randomShortAlphabetString();

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .messageType(messageType)
            .build();
        byte[] body = RandomUtil.randomShortByteArray();
        callbackRef.get().messageArrived(properties, body);
        Thread.sleep(100);

        // then
        verify(handler, atLeast(1)).handleFire(any());
        verifyNoMoreInteractions(handler);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verifyNoMoreInteractions(mqttClient);
        sut.close();
    }

    @Test
    public void handleFireThrowEzyMqttConnectionLostExceptionTest() throws Exception {
        // given
        String topic = RandomUtil.randomShortAlphabetString();

        AtomicReference<EzyMqttCallback> callbackRef = new AtomicReference<>();
        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);
        doAnswer(it -> {
            callbackRef.set(it.getArgumentAt(1, EzyMqttCallback.class));
            return null;
        }).when(mqttClient).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );

        EzyMosquittoRpcCallHandler handler = mock(EzyMosquittoRpcCallHandler.class);
        EzyMqttConnectionLostException exception = new EzyMqttConnectionLostException(
            new RuntimeException("test")
        );
        doThrow(exception).when(handler).handleFire(any());

        EzyMosquittoRpcServer sut = EzyMosquittoRpcServer.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(null)
            .build();
        sut.setCallHandler(handler);

        // when
        new Thread(sut::start).start();
        Thread.sleep(50);

        String messageType = RandomUtil.randomShortAlphabetString();

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .messageType(messageType)
            .build();
        byte[] body = RandomUtil.randomShortByteArray();
        callbackRef.get().messageArrived(properties, body);
        Thread.sleep(100);

        // then
        verify(handler, atLeast(1)).handleFire(any());
        verifyNoMoreInteractions(handler);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verifyNoMoreInteractions(mqttClient);
        sut.close();
    }

    @Test
    public void handleFireThrowEzyMqttExceptionTest() throws Exception {
        // given
        String topic = RandomUtil.randomShortAlphabetString();

        AtomicReference<EzyMqttCallback> callbackRef = new AtomicReference<>();
        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);
        doAnswer(it -> {
            callbackRef.set(it.getArgumentAt(1, EzyMqttCallback.class));
            return null;
        }).when(mqttClient).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );

        EzyMosquittoRpcCallHandler handler = mock(EzyMosquittoRpcCallHandler.class);
        RuntimeException exception = new RuntimeException("test");
        doThrow(exception).when(handler).handleFire(any());

        EzyMosquittoRpcServer sut = EzyMosquittoRpcServer.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .replyTopic(null)
            .build();
        sut.setCallHandler(handler);

        // when
        new Thread(sut::start).start();
        Thread.sleep(50);

        String messageType = RandomUtil.randomShortAlphabetString();

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .messageType(messageType)
            .build();
        byte[] body = RandomUtil.randomShortByteArray();
        callbackRef.get().messageArrived(properties, body);
        Thread.sleep(100);

        // then
        verify(handler, atLeast(1)).handleFire(any());
        verifyNoMoreInteractions(handler);

        verify(mqttClient, times(1)).registerCallback(
            anyString(),
            any(EzyMqttCallback.class)
        );
        verifyNoMoreInteractions(mqttClient);
        sut.close();
    }
}
