package com.tvd12.ezymq.mosquitto;

import com.tvd12.ezymq.common.EzyMQRpcProxyBuilder;
import com.tvd12.ezymq.mosquitto.annotation.EzyMosquittoConsumer;
import com.tvd12.ezymq.mosquitto.annotation.EzyMosquittoHandler;
import com.tvd12.ezymq.mosquitto.annotation.EzyMosquittoInterceptor;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientFactory;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientFactoryBuilder;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoSettings;

public class EzyMosquittoProxyBuilder extends EzyMQRpcProxyBuilder<
    EzyMosquittoSettings,
    EzyMosquittoProxy,
    EzyMosquittoProxyBuilder
    > {

    protected EzyMqttClientFactory mqttClientFactory;

    @Override
    public EzyMosquittoSettings.Builder settingsBuilder() {
        return (EzyMosquittoSettings.Builder) super.settingsBuilder();
    }

    @Override
    protected EzyMosquittoSettings.Builder newSettingBuilder() {
        return new EzyMosquittoSettings.Builder(this);
    }

    public EzyMosquittoProxyBuilder mqttClientFactory(
        EzyMqttClientFactory mqttClientFactory
    ) {
        this.mqttClientFactory = mqttClientFactory;
        return this;
    }
    
    @Override
    public Class<?> getRequestInterceptorAnnotationClass() {
        return EzyMosquittoInterceptor.class;
    }

    @Override
    public Class<?> getRequestHandlerAnnotationClass() {
        return EzyMosquittoHandler.class;
    }

    @Override
    protected Class<?> getMessageConsumerAnnotationClass() {
        return EzyMosquittoConsumer.class;
    }

    @Override
    protected void preNewProxy() {
        if (mqttClientFactory == null) {
            mqttClientFactory = new EzyMqttClientFactoryBuilder()
                .properties(settings.getProperties())
                .build();
        }
    }

    @Override
    protected EzyMosquittoProxy newProxy() {
        return new EzyMosquittoProxy(
            mqttClientFactory.newMqttClient(),
            settings,
            dataCodec,
            entityCodec
        );
    }
}
