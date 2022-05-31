package com.tvd12.ezymq.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezymq.common.EzyMQRpcProxyBuilder;
import com.tvd12.ezymq.rabbitmq.annotation.EzyRabbitConsumer;
import com.tvd12.ezymq.rabbitmq.annotation.EzyRabbitHandler;
import com.tvd12.ezymq.rabbitmq.annotation.EzyRabbitInterceptor;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitConnectionFactoryBuilder;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitSettings;

public class EzyRabbitMQProxyBuilder extends EzyMQRpcProxyBuilder<
    EzyRabbitSettings,
    EzyRabbitMQProxy,
    EzyRabbitMQProxyBuilder
    > {

    protected ConnectionFactory connectionFactory;

    @Override
    public EzyRabbitSettings.Builder settingsBuilder() {
        return (EzyRabbitSettings.Builder) super.settingsBuilder();
    }

    @Override
    protected EzyRabbitSettings.Builder newSettingBuilder() {
        return new EzyRabbitSettings.Builder(this);
    }
    
    public EzyRabbitMQProxyBuilder connectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
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
        if (connectionFactory == null) {
            connectionFactory = new EzyRabbitConnectionFactoryBuilder()
                .properties(settings.getProperties())
                .build();
        }
    }

    @Override
    protected EzyRabbitMQProxy newProxy() {
        return new EzyRabbitMQProxy(
            settings,
            dataCodec,
            entityCodec,
            connectionFactory
        );
    }
}
