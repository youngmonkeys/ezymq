package com.tvd12.ezymq.activemq;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezyfox.util.EzyCloseable;
import com.tvd12.ezymq.activemq.codec.EzyActiveDataCodec;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveConnectionFactory;
import com.tvd12.ezymq.activemq.manager.EzyActiveRpcProducerManager;
import com.tvd12.ezymq.activemq.manager.EzyActiveRpcConsumerManager;
import com.tvd12.ezymq.activemq.manager.EzyActiveTopicManager;
import com.tvd12.ezymq.activemq.setting.EzyActiveSettings;

import javax.jms.ConnectionFactory;

public class EzyActiveMQProxy implements EzyCloseable {

    protected final EzyActiveSettings settings;
    protected final EzyEntityCodec entityCodec;
    protected final EzyActiveDataCodec dataCodec;
    protected final EzyActiveTopicManager topicManager;
    protected final ConnectionFactory connectionFactory;
    protected final EzyActiveRpcProducerManager rpcProducerManager;
    protected final EzyActiveRpcConsumerManager rpcConsumerManager;

    public EzyActiveMQProxy(
        EzyEntityCodec entityCodec,
        EzyActiveDataCodec dataCodec,
        EzyActiveSettings settings,
        ConnectionFactory connectionFactory
    ) {
        this.settings = settings;
        this.dataCodec = dataCodec;
        this.entityCodec = entityCodec;
        this.connectionFactory = connectionFactory;
        this.topicManager = newTopicManager();
        this.rpcProducerManager = newRpcProducerManager();
        this.rpcConsumerManager = newActiveRpcConsumerManager();
    }

    public static EzyActiveMQProxyBuilder builder() {
        return new EzyActiveMQProxyBuilder();
    }

    public <T> EzyActiveTopic<T> getTopic(String name) {
        return topicManager.getTopic(name);
    }

    public EzyActiveRpcProducer getRpcProducer(String name) {
        return rpcProducerManager.getRpcProducer(name);
    }

    public EzyActiveRpcConsumer getActiveRpcConsumer(String name) {
        return rpcConsumerManager.getRpcConsumer(name);
    }

    @Override
    public void close() {
        topicManager.close();
        rpcConsumerManager.close();
        rpcProducerManager.close();
        if (connectionFactory instanceof EzyActiveConnectionFactory) {
            ((EzyActiveConnectionFactory) connectionFactory).close();
        }
    }

    protected EzyActiveTopicManager newTopicManager() {
        return new EzyActiveTopicManager(
            dataCodec,
            connectionFactory,
            settings.getTopicSettings()
        );
    }

    protected EzyActiveRpcProducerManager newRpcProducerManager() {
        return new EzyActiveRpcProducerManager(
            entityCodec,
            connectionFactory,
            settings.getRpcProducerSettings()
        );
    }

    protected EzyActiveRpcConsumerManager newActiveRpcConsumerManager() {
        return new EzyActiveRpcConsumerManager(
            dataCodec,
            connectionFactory,
            settings.getRpcConsumerSettings()
        );
    }
}
