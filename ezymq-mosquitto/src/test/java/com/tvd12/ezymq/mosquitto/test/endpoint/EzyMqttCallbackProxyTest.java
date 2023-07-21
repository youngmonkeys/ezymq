package com.tvd12.ezymq.mosquitto.test.endpoint;

import com.tvd12.ezymq.mosquitto.codec.EzyMqttMqMessageCodec;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallback;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallbackProxy;
import com.tvd12.ezymq.mosquitto.message.EzyMqttMqMessage;
import com.tvd12.test.util.RandomUtil;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyMqttCallbackProxyTest {

    @Test
    public void connectionLostTest() {
        // given
        EzyMqttMqMessageCodec mqttMqMessageCodec = mock(EzyMqttMqMessageCodec.class);
        EzyMqttCallbackProxy instance = new EzyMqttCallbackProxy(mqttMqMessageCodec);

        String topic = RandomUtil.randomShortAlphabetString();
        EzyMqttCallback callback = mock(EzyMqttCallback.class);
        instance.registerCallback(topic, callback);

        // when
        Throwable throwable = new Throwable("test");
        instance.connectionLost(throwable);

        // then
        verifyNoMoreInteractions(mqttMqMessageCodec);

        verify(callback, times(1)).connectionLost(
            any()
        );
        verifyNoMoreInteractions(callback);
    }

    @Test
    public void messageArrivedTest() throws Exception {
        // given
        EzyMqttMqMessageCodec mqttMqMessageCodec = mock(EzyMqttMqMessageCodec.class);
        EzyMqttCallbackProxy instance = new EzyMqttCallbackProxy(mqttMqMessageCodec);

        String topic = RandomUtil.randomShortAlphabetString();
        EzyMqttCallback callback = mock(EzyMqttCallback.class);
        instance.registerCallback(topic, callback);

        int messageId = RandomUtil.randomSmallInt();
        String messageType = RandomUtil.randomShortAlphabetString();
        String correlationId = RandomUtil.randomShortAlphabetString();
        byte[] messageBody = RandomUtil.randomShortByteArray();

        EzyMqttMqMessage mqttMqMessage = EzyMqttMqMessage.builder()
            .id(messageId)
            .type(messageType)
            .correlationId(correlationId)
            .body(messageBody)
            .build();


        MqttMessage mqttMessage = mock(MqttMessage.class);
        when(mqttMqMessageCodec.decode(mqttMessage)).thenReturn(mqttMqMessage);

        // when
        instance.messageArrived(
            topic,
            mqttMessage
        );

        // then
        verify(mqttMqMessageCodec, times(1)).decode(mqttMessage);
        verifyNoMoreInteractions(mqttMqMessageCodec);

        verify(callback, times(1)).messageArrived(
            any(),
            any()
        );
        verifyNoMoreInteractions(callback);
    }

    @Test
    public void messageArrivedButCallbackNullTest() {
        // given
        EzyMqttMqMessageCodec mqttMqMessageCodec = mock(EzyMqttMqMessageCodec.class);
        EzyMqttCallbackProxy instance = new EzyMqttCallbackProxy(mqttMqMessageCodec);

        String topic = RandomUtil.randomShortAlphabetString();

        MqttMessage mqttMessage = mock(MqttMessage.class);

        // when
        instance.messageArrived(
            topic,
            mqttMessage
        );

        // then
        verifyNoMoreInteractions(mqttMqMessageCodec);
    }

    @Test
    public void messageArrivedExceptionTest() throws Exception {
        // given
        EzyMqttMqMessageCodec mqttMqMessageCodec = mock(EzyMqttMqMessageCodec.class);
        EzyMqttCallbackProxy instance = new EzyMqttCallbackProxy(mqttMqMessageCodec);

        String topic = RandomUtil.randomShortAlphabetString();
        EzyMqttCallback callback = mock(EzyMqttCallback.class);
        instance.registerCallback(topic, callback);

        MqttMessage mqttMessage = mock(MqttMessage.class);

        RuntimeException exception = new RuntimeException("test");
        when(mqttMqMessageCodec.decode(mqttMessage)).thenThrow(exception);

        // when
        instance.messageArrived(
            topic,
            mqttMessage
        );

        // then
        verify(mqttMqMessageCodec, times(1)).decode(mqttMessage);
        verifyNoMoreInteractions(mqttMqMessageCodec);

        verifyNoMoreInteractions(callback);
    }

    @Test
    public void deliveryCompleteTest() {
        // given
        EzyMqttMqMessageCodec mqttMqMessageCodec = mock(EzyMqttMqMessageCodec.class);
        EzyMqttCallbackProxy instance = new EzyMqttCallbackProxy(mqttMqMessageCodec);

        String topic = RandomUtil.randomShortAlphabetString();
        EzyMqttCallback callback = mock(EzyMqttCallback.class);
        instance.registerCallback(topic, callback);

        IMqttDeliveryToken mqttDeliveryToken = mock(IMqttDeliveryToken.class);

        // when
        instance.deliveryComplete(mqttDeliveryToken);

        // then
        verifyNoMoreInteractions(mqttMqMessageCodec);
        verifyNoMoreInteractions(mqttDeliveryToken);
        verifyNoMoreInteractions(callback);
    }
}
