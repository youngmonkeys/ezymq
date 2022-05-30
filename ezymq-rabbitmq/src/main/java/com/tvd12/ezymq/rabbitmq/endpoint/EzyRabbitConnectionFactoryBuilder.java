package com.tvd12.ezymq.rabbitmq.endpoint;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.impl.ForgivingExceptionHandler;
import com.tvd12.ezyfox.builder.EzyBuilder;
import com.tvd12.ezyfox.io.EzyStrings;
import com.tvd12.ezymq.rabbitmq.concurrent.EzyRabbitThreadFactory;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.tvd12.ezymq.rabbitmq.setting.EzyRabbitSettings.*;

public class EzyRabbitConnectionFactoryBuilder
        implements EzyBuilder<ConnectionFactory> {

    protected String uri;
    protected int port = 5672;
    protected String username = "guest";
    protected String password = "guest";
    protected String host = "localhost";
    protected String vhost = "/";
    protected int requestedHeartbeat = 15;
    protected int sharedThreadPoolSize;
    protected int maxConnectionAttempts;
    protected ThreadFactory threadFactory;
    protected ExceptionHandler exceptionHandler;

    public EzyRabbitConnectionFactoryBuilder uri(String uri) {
        this.uri = uri;
        return this;
    }

    public EzyRabbitConnectionFactoryBuilder host(String host) {
        this.host = host;
        return this;
    }

    public EzyRabbitConnectionFactoryBuilder vhost(String vhost) {
        this.vhost = vhost;
        return this;
    }

    public EzyRabbitConnectionFactoryBuilder port(int port) {
        this.port = port;
        return this;
    }

    public EzyRabbitConnectionFactoryBuilder username(String username) {
        this.username = username;
        return this;
    }

    public EzyRabbitConnectionFactoryBuilder password(String password) {
        this.password = password;
        return this;
    }

    public EzyRabbitConnectionFactoryBuilder threadFactory(ThreadFactory factory) {
        this.threadFactory = factory;
        return this;
    }

    public EzyRabbitConnectionFactoryBuilder threadFactory(
        String poolName
    ) {
        return threadFactory(EzyRabbitThreadFactory.create(poolName));
    }

    public EzyRabbitConnectionFactoryBuilder requestedHeartbeat(
        int requestedHeartbeat
    ) {
        this.requestedHeartbeat = requestedHeartbeat;
        return this;
    }

    public EzyRabbitConnectionFactoryBuilder sharedThreadPoolSize(
        int sharedThreadPoolSize
    ) {
        this.sharedThreadPoolSize = sharedThreadPoolSize;
        return this;
    }

    public EzyRabbitConnectionFactoryBuilder maxConnectionAttempts(
        int maxConnectionAttempts
    ) {
        this.maxConnectionAttempts = maxConnectionAttempts;
        return this;
    }

    public EzyRabbitConnectionFactoryBuilder exceptionHandler(
        ExceptionHandler exceptionHandler
    ) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }

    public EzyRabbitConnectionFactoryBuilder properties(Properties properties) {
        this.maxConnectionAttempts = Integer.parseInt(
            properties
                .getOrDefault(KEY_MAX_CONNECTION_ATTEMPTS, maxConnectionAttempts)
                .toString()
        );
        this.uri = properties.getProperty(KEY_URI, uri);
        this.host = properties.getProperty(KEY_HOST, host);
        this.port = Integer.parseInt(
            properties.getOrDefault(KEY_PORT, port).toString()
        );
        this.username = properties.getProperty(KEY_USERNAME, username);
        this.password = properties.getProperty(KEY_PASSWORD, password);
        this.vhost = properties.getProperty(KEY_VHOST, vhost);
        this.requestedHeartbeat = Integer.parseInt(
            properties
                .getOrDefault(KEY_REQUESTED_HEART_BEAT, requestedHeartbeat)
                .toString()
        );
        this.sharedThreadPoolSize = Integer.parseInt(
            properties
                .getOrDefault(KEY_SHARED_THREAD_POOL_SIZE, sharedThreadPoolSize)
                .toString()
        );
        return this;
    }

    @Override
    public ConnectionFactory build() {
        if (threadFactory == null) {
            threadFactory = newThreadFactory();
        }
        if (exceptionHandler == null) {
            exceptionHandler = newExceptionHandler();
        }
        EzyRabbitConnectionFactory factory = new EzyRabbitConnectionFactory();
        if (EzyStrings.isNoContent(uri)) {
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setVirtualHost(vhost);
        } else {
            setConnectionURI(factory);
        }
        factory.setThreadFactory(threadFactory);
        factory.setExceptionHandler(exceptionHandler);
        if (requestedHeartbeat > 0) {
            factory.setRequestedHeartbeat(requestedHeartbeat);
        }
        if (sharedThreadPoolSize > 0) {
            factory.setSharedExecutor(Executors.newFixedThreadPool(sharedThreadPoolSize, threadFactory));
        }
        if (maxConnectionAttempts > 0) {
            factory.setMaxConnectionAttempts(maxConnectionAttempts);
        }
        return factory;
    }

    private void setConnectionURI(ConnectionFactory connectionFactory) {
        try {
            connectionFactory.setUri(uri);
        } catch (Exception e) {
            throw new IllegalArgumentException("uri: " + uri + " is invalid", e);
        }
    }

    private ThreadFactory newThreadFactory() {
        return EzyRabbitThreadFactory.create("worker");
    }

    protected ExceptionHandler newExceptionHandler() {
        return new ForgivingExceptionHandler();
    }
}
