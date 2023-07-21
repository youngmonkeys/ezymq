package com.tvd12.ezymq.mosquitto.test;

import com.tvd12.ezymq.mosquitto.EzyMosquittoProxy;
import com.tvd12.ezymq.mosquitto.EzyMosquittoProxyBuilder;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientFactory;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientProxy;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoSettings;
import com.tvd12.test.assertion.Asserts;
import com.tvd12.test.reflect.FieldUtil;
import com.tvd12.test.reflect.MethodInvoker;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.mockito.Mockito.*;

public class EzyMosquittoProxyBuilderTest {

    @Test
    public void buildTest() {
        // given
        EzyMqttClientFactory mqttClientFactory =
            mock(EzyMqttClientFactory.class);
        EzyMqttClientProxy mqttClientProxy = mock(EzyMqttClientProxy.class);
        when(mqttClientFactory.newMqttClient()).thenReturn(mqttClientProxy);

        // when
        EzyMosquittoProxy proxy = EzyMosquittoProxy.builder()
            .mqttClientFactory(mqttClientFactory)
            .build();

        // then
        verify(mqttClientFactory, times(1)).newMqttClient();
        verifyNoMoreInteractions(mqttClientFactory);
        proxy.close();
    }

    @Test
    public void preNewProxyTest() {
        // given
        EzyMosquittoSettings settings = mock(EzyMosquittoSettings.class);
        when(settings.getProperties()).thenReturn(new Properties());
        EzyMosquittoProxyBuilder instance = new EzyMosquittoProxyBuilder()
            .settingsBuilder()
            .parent()
            .settings(settings);

        // when
        MethodInvoker.create()
            .object(instance)
            .method("preNewProxy")
            .invoke();

        // then
        EzyMqttClientFactory mqttClientFactory = FieldUtil.getFieldValue(
            instance,
            "mqttClientFactory"
        );
        Asserts.assertNotNull(mqttClientFactory);
    }
}
