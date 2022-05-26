package com.tvd12.ezymq.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.rabbitmq.codec.EzyRabbitDataCodec;
import com.tvd12.ezymq.rabbitmq.endpoint.EzyRabbitConnectionFactory;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitRpcProducerManager;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitRpcConsumerManager;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitTopicManager;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitSettings;

public class EzyRabbitMQProxy implements EzyCloseable {

    protected final EzyRabbitSettings settings;
    protected final EzyEntityCodec entityCodec;
    protected final EzyRabbitDataCodec dataCodec;
    protected final EzyRabbitTopicManager topicManager;
    protected final ConnectionFactory connectionFactory;
    protected final EzyRabbitRpcProducerManager rpcProducerManager;
    protected final EzyRabbitRpcConsumerManager rpcConsumerManager;

    public EzyRabbitMQProxy(
        EzyEntityCodec entityCodec,
        EzyRabbitDataCodec dataCodec,
        EzyRabbitSettings settings,
        ConnectionFactory connectionFactory
    ) {
        this.settings = settings;
        this.dataCodec = dataCodec;
        this.entityCodec = entityCodec;
        this.connectionFactory = connectionFactory;
        this.topicManager = newTopicManager();
        this.rpcProducerManager = newRpcProducerManager();
        this.rpcConsumerManager = newRabbitRpcConsumerManager();

    }

    public static EzyRabbitMQProxyBuilder builder() {
        return new EzyRabbitMQProxyBuilder();
    }

    public <T> EzyRabbitTopic<T> getTopic(String name) {
        return topicManager.getTopic(name);
    }

    public EzyRabbitRpcProducer getRpcProducer(String name) {
        return rpcProducerManager.getRpcProducer(name);
    }

    public EzyRabbitRpcConsumer getRabbitRpcConsumer(String name) {
        return rpcConsumerManager.getRpcConsumer(name);
    }

    @Override
    public void close() {
        rpcConsumerManager.close();
        rpcProducerManager.close();
        if (connectionFactory instanceof EzyRabbitConnectionFactory) {
            ((EzyRabbitConnectionFactory) connectionFactory).close();
        }
    }

    protected EzyRabbitTopicManager newTopicManager() {
        return new EzyRabbitTopicManager(
            dataCodec,
            connectionFactory,
            settings.getQueueArguments(),
            settings.getTopicSettings()
        );
    }

    protected EzyRabbitRpcProducerManager newRpcProducerManager() {
        return new EzyRabbitRpcProducerManager(
            entityCodec,
            connectionFactory,
            settings.getQueueArguments(),
            settings.getRpcProducerSettings()
        );
    }

    protected EzyRabbitRpcConsumerManager newRabbitRpcConsumerManager() {
        return new EzyRabbitRpcConsumerManager(
            dataCodec,
            connectionFactory,
            settings.getRpcConsumerSettings()
        );
    }
}
