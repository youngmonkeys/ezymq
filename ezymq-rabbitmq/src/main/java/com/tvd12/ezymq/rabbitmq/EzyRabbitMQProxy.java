package com.tvd12.ezymq.rabbitmq;

import com.rabbitmq.client.Connection;
import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.common.EzyMQRpcProxy;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitRpcConsumerManager;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitRpcProducerManager;
import com.tvd12.ezymq.rabbitmq.manager.EzyRabbitTopicManager;
import com.tvd12.ezymq.rabbitmq.setting.EzyRabbitSettings;

import java.io.IOException;

public class EzyRabbitMQProxy extends EzyMQRpcProxy<EzyRabbitSettings> {

    protected final Connection connection;
    protected final EzyRabbitTopicManager topicManager;
    protected final EzyRabbitRpcProducerManager rpcProducerManager;
    protected final EzyRabbitRpcConsumerManager rpcConsumerManager;

    public EzyRabbitMQProxy(
        Connection connection,
        EzyRabbitSettings settings,
        EzyMQDataCodec dataCodec,
        EzyEntityCodec entityCodec
    ) {
        super(settings, dataCodec, entityCodec);
        this.connection = connection;
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

    public EzyRabbitRpcConsumer getRpcConsumer(String name) {
        return rpcConsumerManager.getRpcConsumer(name);
    }

    @Override
    public void close() throws IOException {
        topicManager.close();
        rpcConsumerManager.close();
        rpcProducerManager.close();
        connection.close();
    }

    protected EzyRabbitTopicManager newTopicManager() {
        return new EzyRabbitTopicManager(
            connection,
            dataCodec,
            settings.getQueueArguments(),
            settings.getTopicSettings()
        );
    }

    protected EzyRabbitRpcProducerManager newRpcProducerManager() {
        return new EzyRabbitRpcProducerManager(
            connection,
            entityCodec,
            settings.getQueueArguments(),
            settings.getRpcProducerSettings()
        );
    }

    protected EzyRabbitRpcConsumerManager newRabbitRpcConsumerManager() {
        return new EzyRabbitRpcConsumerManager(
            connection,
            dataCodec,
            settings.getRpcConsumerSettings()
        );
    }
}
