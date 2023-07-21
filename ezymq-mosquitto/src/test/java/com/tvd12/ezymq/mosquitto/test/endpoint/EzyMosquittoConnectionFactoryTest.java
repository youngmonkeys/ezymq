package com.tvd12.ezymq.mosquitto.test.endpoint;

import com.tvd12.ezymq.mosquitto.codec.EzyMqttMqMessageCodec;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientFactory;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

public class EzyMosquittoConnectionFactoryTest extends BaseTest {

    @Test
    public void newConnectionSuccessfullyButRetry() throws Exception {
        // given
        String serverUri = "tcp://helloworld";
        String clientIdPrefix = RandomUtil.randomShortAlphabetString();
        String username = RandomUtil.randomShortAlphabetString();
        String password = RandomUtil.randomShortAlphabetString();
        int maxConnectionAttempts = RandomUtil.randomInt(2, 10);
        int connectionAttemptSleepTime = 100;
        EzyMqttMqMessageCodec mqttMqMessageCodec = mock(EzyMqttMqMessageCodec.class);

        AtomicInteger retryCount = new AtomicInteger();
        EzyMqttClientProxy mqttClientProxy = mock(EzyMqttClientProxy.class);
        doAnswer(it -> {
            int count = retryCount.incrementAndGet();
            if (count == 1) {
                throw new RuntimeException("test");
            }
            return null;
        }).when(mqttClientProxy).connect();

        EzyMqttClientFactory sut = new EzyMqttClientFactory(
            serverUri,
            clientIdPrefix,
            username,
            password,
            maxConnectionAttempts,
            connectionAttemptSleepTime,
            mqttMqMessageCodec
        ) {
            @Override
            protected EzyMqttClientProxy neMqttClientProxy(MqttClient mqttClient) {
                return mqttClientProxy;
            }
        };

        // when
        EzyMqttClientProxy client = sut.newMqttClient();

        // then
        Asserts.assertNotNull(client);

        verify(mqttClientProxy, times(2)).connect();
        verifyNoMoreInteractions(mqttClientProxy);
        sut.close();
    }

    @Test
    public void newConnectionFailed() {
        // given
        String serverUri = "tcp://helloworld";
        String clientIdPrefix = RandomUtil.randomShortAlphabetString();
        int maxConnectionAttempts = RandomUtil.randomInt(2, 10);
        int connectionAttemptSleepTime = 100;
        EzyMqttMqMessageCodec mqttMqMessageCodec = mock(EzyMqttMqMessageCodec.class);

        EzyMqttClientFactory sut = new EzyMqttClientFactory(
            serverUri,
            clientIdPrefix,
            null,
            null,
            maxConnectionAttempts,
            connectionAttemptSleepTime,
            mqttMqMessageCodec
        );

        // when
        Throwable e = Asserts.assertThrows(sut::newMqttClient);

        // then
        Asserts.assertEqualsType(e, IllegalStateException.class);
        sut.close();
    }
}
