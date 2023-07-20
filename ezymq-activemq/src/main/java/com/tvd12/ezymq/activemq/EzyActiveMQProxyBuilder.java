package com.tvd12.ezymq.activemq;

import com.tvd12.ezymq.activemq.annotation.EzyActiveConsumer;
import com.tvd12.ezymq.activemq.annotation.EzyActiveHandler;
import com.tvd12.ezymq.activemq.annotation.EzyActiveInterceptor;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveConnectionFactoryBuilder;
import com.tvd12.ezymq.activemq.setting.EzyActiveSettings;
import com.tvd12.ezymq.common.EzyMQRpcProxyBuilder;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

public class EzyActiveMQProxyBuilder extends EzyMQRpcProxyBuilder<
    EzyActiveSettings,
    EzyActiveMQProxy,
    EzyActiveMQProxyBuilder
    > {

    protected ConnectionFactory connectionFactory;

    @Override
    public EzyActiveSettings.Builder settingsBuilder() {
        return (EzyActiveSettings.Builder) super.settingsBuilder();
    }

    @Override
    protected EzyActiveSettings.Builder newSettingBuilder() {
        return new EzyActiveSettings.Builder(this);
    }

    public EzyActiveMQProxyBuilder connectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        return this;
    }

    @Override
    public Class<?> getRequestInterceptorAnnotationClass() {
        return EzyActiveInterceptor.class;
    }

    @Override
    public Class<?> getRequestHandlerAnnotationClass() {
        return EzyActiveHandler.class;
    }

    @Override
    protected Class<?> getMessageConsumerAnnotationClass() {
        return EzyActiveConsumer.class;
    }

    @Override
    protected void preNewProxy() {
        if (connectionFactory == null) {
            connectionFactory = new EzyActiveConnectionFactoryBuilder()
                .properties(settings.getProperties())
                .build();
        }
    }

    @Override
    protected EzyActiveMQProxy newProxy() {
        Connection connection;
        try {
            connection = connectionFactory.createConnection();
        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
        return new EzyActiveMQProxy(
            connection,
            settings,
            dataCodec,
            entityCodec
        );
    }
}
