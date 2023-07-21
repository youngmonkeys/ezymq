package com.tvd12.ezymq.mosquitto.test.endpoint;

import com.tvd12.ezymq.mosquitto.codec.EzyMqttMqMessageCodec;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallback;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.message.EzyMqttMqMessage;
import com.tvd12.test.util.RandomUtil;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyMqttClientProxyTest {

    @Test
    public void registerCallbackTest() throws Exception {
        // given
        MqttClient mqttClient = mock(MqttClient.class);
        EzyMqttMqMessageCodec mqttMqMessageCodec = mock(EzyMqttMqMessageCodec.class);

        EzyMqttClientProxy instance = new EzyMqttClientProxy(
            mqttClient,
            mqttMqMessageCodec
        );

        String topic = RandomUtil.randomShortAlphabetString();
        EzyMqttCallback callback = mock(EzyMqttCallback.class);

        // when
        instance.registerCallback(topic, callback);
        instance.close();

        // then
        verify(mqttClient, times(1)).setCallback(any());
        verify(mqttClient, times(1)).close();
        verifyNoMoreInteractions(mqttClient);

        verifyNoMoreInteractions(mqttMqMessageCodec);
        verifyNoMoreInteractions(callback);
    }

    @Test
    public void connectTest() throws Exception {
        // given
        MqttClient mqttClient = mock(MqttClient.class);
        EzyMqttMqMessageCodec mqttMqMessageCodec = mock(EzyMqttMqMessageCodec.class);

        EzyMqttClientProxy instance = new EzyMqttClientProxy(
            mqttClient,
            mqttMqMessageCodec
        );

        EzyMqttMqMessage message = mock(EzyMqttMqMessage.class);

        // when
        instance.connect();

        // then
        verify(mqttClient, times(1)).setCallback(any());
        verify(mqttClient, times(1)).connect();
        verifyNoMoreInteractions(mqttClient);

        verifyNoMoreInteractions(mqttMqMessageCodec);
        verifyNoMoreInteractions(message);
    }

    @Test
    public void publishTest() throws Exception {
        // given
        MqttClient mqttClient = mock(MqttClient.class);
        EzyMqttMqMessageCodec mqttMqMessageCodec = mock(EzyMqttMqMessageCodec.class);

        EzyMqttClientProxy instance = new EzyMqttClientProxy(
            mqttClient,
            mqttMqMessageCodec
        );

        String topic = RandomUtil.randomShortAlphabetString();
        EzyMqttMqMessage message = mock(EzyMqttMqMessage.class);

        // when
        instance.publish(topic, message);

        // then
        verify(mqttClient, times(1)).setCallback(any());
        verify(mqttClient, times(1)).publish(any(), any());
        verifyNoMoreInteractions(mqttClient);

        verify(mqttMqMessageCodec, times(1)).encode(
            any()
        );
        verifyNoMoreInteractions(mqttMqMessageCodec);
        verifyNoMoreInteractions(message);
    }

    @Test
    public void subscribeTest() throws Exception {
        // given
        MqttClient mqttClient = mock(MqttClient.class);
        EzyMqttMqMessageCodec mqttMqMessageCodec = mock(EzyMqttMqMessageCodec.class);

        EzyMqttClientProxy instance = new EzyMqttClientProxy(
            mqttClient,
            mqttMqMessageCodec
        );

        String topic = RandomUtil.randomShortAlphabetString();
        EzyMqttMqMessage message = mock(EzyMqttMqMessage.class);

        // when
        instance.subscribe(topic);

        // then
        verify(mqttClient, times(1)).setCallback(any());
        verify(mqttClient, times(1)).subscribe(topic);
        verifyNoMoreInteractions(mqttClient);

        verifyNoMoreInteractions(mqttMqMessageCodec);
        verifyNoMoreInteractions(message);
    }
}
