package com.tvd12.ezymq.mosquitto.test.endpoint;

import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoTopicClient;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.message.EzyMqttMqMessage;
import com.tvd12.ezymq.mosquitto.util.EzyMosquittoProperties;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class EzyMosquittoTopicClientTest extends BaseTest {

    @Test
    public void publishTest() throws Exception {
        // given
        String topic = RandomUtil.randomShortAlphabetString();
        EzyMqttClientProxy mqttClient = mock(EzyMqttClientProxy.class);

        EzyMosquittoTopicClient sut = EzyMosquittoTopicClient.builder()
            .mqttClient(mqttClient)
            .topic(topic)
            .build();

        // when
        EzyMosquittoProperties props = EzyMosquittoProperties.builder()
            .build();
        byte[] message = RandomUtil.randomShortByteArray();
        sut.publish(props, message);

        // then
        verify(mqttClient, times(1)).publish(
            anyString(),
            any(EzyMqttMqMessage.class)
        );
        sut.close();
    }
}
