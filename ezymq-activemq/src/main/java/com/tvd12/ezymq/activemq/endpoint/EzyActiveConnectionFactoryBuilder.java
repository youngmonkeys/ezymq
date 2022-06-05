package com.tvd12.ezymq.activemq.endpoint;

import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.io.EzyStrings;
import com.tvd12.ezymq.activemq.handler.EzyActiveExceptionListener;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import java.util.Properties;

import static com.tvd12.ezymq.activemq.setting.EzyActiveSettings.*;

public class EzyActiveConnectionFactoryBuilder implements EzyBuilder<ConnectionFactory> {

    protected String uri;
    protected String username;
    protected String password;
    protected int maxThreadPoolSize;
    protected ExceptionListener exceptionListener;

    public EzyActiveConnectionFactoryBuilder uri(String uri) {
        this.uri = uri;
        return this;
    }

    public EzyActiveConnectionFactoryBuilder username(String username) {
        this.username = username;
        return this;
    }

    public EzyActiveConnectionFactoryBuilder password(String password) {
        this.password = password;
        return this;
    }

    public EzyActiveConnectionFactoryBuilder maxThreadPoolSize(
        int maxThreadPoolSize
    ) {
        this.maxThreadPoolSize = maxThreadPoolSize;
        return this;
    }

    public EzyActiveConnectionFactoryBuilder exceptionListener(
        ExceptionListener exceptionListener
    ) {
        this.exceptionListener = exceptionListener;
        return this;
    }

    public EzyActiveConnectionFactoryBuilder properties(Properties properties) {
        this.uri = properties.getProperty(KEY_URI, uri);
        this.username = properties.getProperty(KEY_USERNAME, username);
        this.password = properties.getProperty(KEY_PASSWORD, password);
        this.maxThreadPoolSize = Integer.parseInt(
            properties
                .getOrDefault(KEY_MAX_THREAD_POOL_SIZE, maxThreadPoolSize)
                .toString()
        );
        return this;
    }

    @Override
    public ConnectionFactory build() {
        if (exceptionListener == null) {
            exceptionListener = newExceptionListener();
        }
        ActiveMQConnectionFactory factory = new EzyActiveConnectionFactory();
        if (!EzyStrings.isNoContent(uri)) {
            setConnectionURI(factory);
        }
        if (!EzyStrings.isEmpty(username)) {
            factory.setUserName(username);
        }
        if (!EzyStrings.isEmpty(password)) {
            factory.setPassword(password);
        }
        if (maxThreadPoolSize > 0) {
            factory.setMaxThreadPoolSize(maxThreadPoolSize);
        }
        factory.setExceptionListener(exceptionListener);
        return factory;
    }

    private void setConnectionURI(ActiveMQConnectionFactory connectionFactory) {
        connectionFactory.setBrokerURL(uri);
    }

    protected ExceptionListener newExceptionListener() {
        return new EzyActiveExceptionListener();
    }
}
