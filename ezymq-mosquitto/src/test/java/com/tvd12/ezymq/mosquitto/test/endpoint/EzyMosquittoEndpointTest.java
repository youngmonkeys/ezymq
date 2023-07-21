package com.tvd12.ezymq.mosquitto.test.endpoint;

import com.tvd12.ezymq.mosquitto.endpoint.EzyMosquittoEndpoint;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.base.BaseTest;
import com.tvd12.test.util.RandomUtil;
import org.testng.annotations.Test;

import javax.print.attribute.standard.Destination;

import static org.mockito.Mockito.*;

public class EzyMosquittoEndpointTest extends BaseTest {

    @Test
    public void buildTest() {
        // given
        String topic = RandomUtil.randomShortAlphabetString();
        EzyMqttClientProxy mqttClientProxy = mock(EzyMqttClientProxy.class);

        // when
        EzyMosquittoEndpoint endpoint = new InternalBuilder()
            .mqttClient(mqttClientProxy)
            .topic(topic)
            .build();

        // then
        verifyNoMoreInteractions(mqttClientProxy);
        endpoint.close();
    }

    private static class InternalBuilder
        extends EzyMosquittoEndpoint.Builder<InternalBuilder> {

        @Override
        public EzyMosquittoEndpoint build() {
            return new EzyMosquittoEndpoint(mqttClient, topic);
        }
    }
}
