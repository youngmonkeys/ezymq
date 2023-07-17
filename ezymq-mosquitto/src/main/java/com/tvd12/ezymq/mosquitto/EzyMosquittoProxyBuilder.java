package com.tvd12.ezymq.mosquitto;

import com.tvd12.ezymq.common.EzyMQRpcProxyBuilder;
import com.tvd12.ezymq.mosquitto.annotation.EzyRabbitConsumer;
import com.tvd12.ezymq.mosquitto.annotation.EzyRabbitHandler;
import com.tvd12.ezymq.mosquitto.annotation.EzyRabbitInterceptor;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttCallbackProxy;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientFactory;
import com.tvd12.ezymq.mosquitto.endpoint.EzyMqttClientFactoryBuilder;
import com.tvd12.ezymq.mosquitto.setting.EzyMosquittoSettings;

public class EzyMosquittoProxyBuilder extends EzyMQRpcProxyBuilder<
    EzyMosquittoSettings,
    EzyMosquittoProxy,
    EzyMosquittoProxyBuilder
    > {

    protected EzyMqttCallbackProxy mqttCallbackProxy;
    protected EzyMqttClientFactory mqttClientFactory;

    @Override
    public EzyMosquittoSettings.Builder settingsBuilder() {
        return (EzyMosquittoSettings.Builder) super.settingsBuilder();
    }

    @Override
    protected EzyMosquittoSettings.Builder newSettingBuilder() {
        return new EzyMosquittoSettings.Builder(this);
    }

    public EzyMosquittoProxyBuilder connectionFactory(
        EzyMqttClientFactory mqttClientFactory
    ) {
        this.mqttClientFactory = mqttClientFactory;
        return this;
    }
    
    public EzyMosquittoProxyBuilder mqttCallbackProxy(
        EzyMqttCallbackProxy mqttCallbackProxy
    ) {
        this.mqttCallbackProxy = mqttCallbackProxy;
        return this;
    }

    @Override
    public Class<?> getRequestInterceptorAnnotationClass() {
        return EzyRabbitInterceptor.class;
    }

    @Override
    public Class<?> getRequestHandlerAnnotationClass() {
        return EzyRabbitHandler.class;
    }

    @Override
    protected Class<?> getMessageConsumerAnnotationClass() {
        return EzyRabbitConsumer.class;
    }

    @Override
    protected void preNewProxy() {
        if (mqttClientFactory == null) {
            mqttClientFactory = new EzyMqttClientFactoryBuilder()
                .properties(settings.getProperties())
                .build();
        }
        if (mqttCallbackProxy == null) {
            mqttCallbackProxy = new EzyMqttCallbackProxy();
        }
    }

    @Override
    protected EzyMosquittoProxy newProxy() {
        return new EzyMosquittoProxy(
            mqttClientFactory.newMqttClient(mqttCallbackProxy),
            mqttCallbackProxy,
            settings,
            dataCodec,
            entityCodec
        );
    }
}
