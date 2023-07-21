package com.tvd12.ezymq.mosquitto.test.endpoint;

import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoTopicServer;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallback;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.handler.EzyMosquittoMessageHandler;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

public class EzyMosquittoTopicServerTest extends BaseTest {

    @Test
    public void startTest() throws Exception {
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

        EzyMosquittoTopicServer sut = EzyMosquittoTopicServer.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .build();

        EzyMosquittoMessageHandler handler = mock(EzyMosquittoMessageHandler.class);
        sut.setMessageHandler(handler);

        EzyMosquittoProperties properties = EzyMosquittoProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();

        // when
        sut.start();
        callbackRef.get().messageArrived(properties, message);
        // then
        verify(handler, atLeast(1)).handle(
            properties,
            message
        );

        sut.close();
    }
}
