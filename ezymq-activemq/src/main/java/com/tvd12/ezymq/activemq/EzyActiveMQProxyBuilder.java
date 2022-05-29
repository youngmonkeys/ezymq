package com.tvd12.ezymq.activemq;

import com.tvd12.ezymq.activemq.annotation.EzyActiveHandler;
import com.tvd12.ezymq.activemq.annotation.EzyActiveInterceptor;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveConnectionFactoryBuilder;
import com.tvd12.ezymq.activemq.setting.EzyActiveSettings;
import com.tvd12.ezymq.common.EzyMQRpcProxyBuilder;

import javax.jms.ConnectionFactory;

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
    public Class<?> getRequestInterceptorAnnotation() {
        return EzyActiveInterceptor.class;
    }

    @Override
    public Class<?> getRequestHandlerAnnotation() {
        return EzyActiveHandler.class;
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
        return new EzyActiveMQProxy(
            settings,
            dataCodec,
            entityCodec,
            connectionFactory
        );
    }
}
