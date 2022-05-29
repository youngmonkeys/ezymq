package com.tvd12.ezymq.activemq;

import com.tvd12.ezyfox.codec.EzyEntityCodec;
import com.tvd12.ezymq.activemq.endpoint.EzyActiveConnectionFactory;
import com.tvd12.ezymq.activemq.manager.EzyActiveRpcConsumerManager;
import com.tvd12.ezymq.activemq.manager.EzyActiveRpcProducerManager;
import com.tvd12.ezymq.activemq.manager.EzyActiveTopicManager;
import com.tvd12.ezymq.activemq.setting.EzyActiveSettings;
import com.tvd12.ezymq.common.EzyMQRpcProxy;
import com.tvd12.ezymq.common.codec.EzyMQDataCodec;

import javax.jms.ConnectionFactory;

public class EzyActiveMQProxy extends EzyMQRpcProxy<EzyActiveSettings> {

    protected final EzyActiveTopicManager topicManager;
    protected final ConnectionFactory connectionFactory;
    protected final EzyActiveRpcProducerManager rpcProducerManager;
    protected final EzyActiveRpcConsumerManager rpcConsumerManager;

    public EzyActiveMQProxy(
        EzyActiveSettings settings,
        EzyMQDataCodec dataCodec,
        EzyEntityCodec entityCodec,
        ConnectionFactory connectionFactory
    ) {
        super(
            settings,
            dataCodec,
            entityCodec
        );
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
